/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.hydrator.action.plugin;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.cdap.hydrator.action.common.SFTPActionConfig;
import co.cask.cdap.hydrator.action.common.SFTPConnector;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.ChannelSftp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nullable;

/**
 * An {@link Action} that will copy files from FTP server to the destination directory.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SFTPCopy")
public class SFTPCopyAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(SFTPCopyAction.class);
  private SFTPCopyActionConfig config;
  private KeyValueTable sftpTrackingTable;
  private static final String TRACKING_TABLE_NAME = "SFTPTrackingTable";

  public SFTPCopyAction(SFTPCopyActionConfig config) {
    this.config = config;
  }

  /**
   * Configurations for the FTP copy action plugin.
   */
  public class SFTPCopyActionConfig extends SFTPActionConfig {
    @Description("Directory on the SFTP server which is to be copied.")
    @Macro
    public String srcDirectory;

    @Description("Destination directory to which the files to be copied. If the directory does not exist," +
      " it will be created.")
    @Macro
    public String destDirectory;

    @Description("Boolean flag to determine whether zip files on the FTP server need to be extracted " +
      "on the destination while copying. Defaults to 'true'.")
    @Nullable
    public Boolean extractZipFiles;

    @Description("Name of the variable in which comma separated list of file names that are copied by the " +
      "plugin will be put.")
    @Nullable
    public String variableNameHoldingFileList;

    @Description("Regex to copy only the file names that match. By default, all files will be copied.")
    @Nullable
    public String fileNameRegex;

    @Nullable
    @Description("Option to track files that is already processed by computing MD5 of files already processed. " +
        "Only already un-processed files will be passed to next stage. By default the files are not tracked")
    public String trackFiles;

    public String getSrcDirectory() {
      return srcDirectory;
    }

    public String getDestDirectory() {
      return destDirectory;
    }

    public Boolean getExtractZipFiles() {
      return (extractZipFiles != null) ? extractZipFiles : true;
    }

    public String getVariableNameHoldingFileList() {
      return variableNameHoldingFileList != null ? variableNameHoldingFileList : "sftp.copied.file.names";
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    if (config.trackFiles != null && config.trackFiles.toLowerCase().equals("yes")) {
      // create dataset
      pipelineConfigurer.createDataset(TRACKING_TABLE_NAME, KeyValueTable.class.getName());
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path destination = new Path(config.getDestDirectory());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    destination = fileSystem.makeQualified(destination);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }

    try (SFTPConnector SFTPConnector = new SFTPConnector(config.getHost(), config.getPort(), config.getUserName(),
                                                      config.getPassword(), config.getSSHProperties())) {
      ChannelSftp channelSftp = SFTPConnector.getSftpChannel();

      Vector files = channelSftp.ls(config.getSrcDirectory());

      List<String> filesCopied = new ArrayList<>();
      for (int index = 0; index < files.size(); index++) {
        Object obj = files.elementAt(index);
        if (!(obj instanceof ChannelSftp.LsEntry)) {
          continue;
        }
        ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) obj;
        if (".".equals(entry.getFilename()) || "..".equals(entry.getFilename())) {
          // ignore "." and ".." files
          continue;
        }

        // Ignore files that don't match the given file regex
        if (!Strings.isNullOrEmpty(config.fileNameRegex)) {
          String fileName = entry.getFilename();
          if (!fileName.matches(config.fileNameRegex)) {
            LOG.debug("Skipping file {} since it doesn't match the regex.", fileName);
            continue;
          }
        }

        LOG.info("Downloading file {}", entry.getFilename());
        String completeFileName = config.getSrcDirectory() + "/" + entry.getFilename();

        if (config.getExtractZipFiles() && entry.getFilename().endsWith(".zip")) {
          copyJschZip(channelSftp.get(completeFileName), fileSystem, destination);
        } else {
          Path destinationPath = fileSystem.makeQualified(new Path(destination, entry.getFilename()));
          LOG.debug("Downloading {} to {}", entry.getFilename(), destinationPath.toString());
          try (OutputStream output = fileSystem.create(destinationPath)) {
            InputStream is = channelSftp.get(completeFileName);
            ByteStreams.copy(is, output);
          }
          if (config.trackFiles != null && config.trackFiles.toLowerCase().equals("yes")) {
            // If the file tracking is enabled then check if it is already processed, if so delete it from destination
            byte[] md5 = getMD5(fileSystem, destinationPath);
            String fileProcessed = getFileFromTrackingTable(context, md5);
            if (fileProcessed != null) {
              LOG.info("File {} matches md5 with already ingested file {}. Skipping",
                  entry.getFilename(), fileProcessed);
              deleteFile(fileSystem, destinationPath);
            } else {
              filesCopied.add(completeFileName);
              trackFile(context, md5, entry.getFilename());
            }
          } else {
            filesCopied.add(completeFileName);
          }
        }
      }
      context.getArguments().set(config.getVariableNameHoldingFileList(), Joiner.on(",").join(filesCopied));
      LOG.info("Variables copied to {}.", Joiner.on(",").join(filesCopied));
    }
  }

  private void trackFile(ActionContext context, final byte [] md5, final String path)
      throws TransactionFailureException {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        KeyValueTable table = (KeyValueTable) datasetContext.getDataset(TRACKING_TABLE_NAME);
        table.write(md5, Bytes.toBytes(path));
      }
    });
  }

  private byte[] getMD5(FileSystem fileSystem, Path path) throws IOException, NoSuchAlgorithmException {
    try (InputStream is = fileSystem.open(path)) {
      DigestInputStream md5 = new DigestInputStream(is, MessageDigest.getInstance("MD5"));
      return md5.getMessageDigest().digest();
    }
  }

  @Nullable
  /**
   * Returns previous processed file name.
   */
  private String getFileFromTrackingTable(final ActionContext context, final byte[] key)
      throws TransactionFailureException {
    final String[] fileProcessed = {null};
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        KeyValueTable table = (KeyValueTable) datasetContext.getDataset(TRACKING_TABLE_NAME);
        byte[] val = table.read(key);
        fileProcessed[0] = (val == null) ? null : new String(val, "UTF-8");
      }
    });
    return fileProcessed[0];
  }

  private void deleteFile (FileSystem fileSystem, Path path) throws IOException {
    if (fileSystem.exists(path)) {
      boolean deleteStatus = fileSystem.delete(path, false);
      if (!deleteStatus) {
        throw new IOException(String.format("Failed to delete file at path %s", path.toString()));
      }
    }
  }

  private void copyJschZip(InputStream is, FileSystem fs, Path destination) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        LOG.debug("Extracting {}", entry);
        Path destinationPath = fs.makeQualified(new Path(destination, entry.getName()));
        try (OutputStream os = fs.create(destinationPath)) {
          LOG.debug("Downloading {} to {}", entry.getName(), destinationPath.toString());
          ByteStreams.copy(zis, os);
        }
      }
    }
  }
}
