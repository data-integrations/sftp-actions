/*
 * Copyright © 2020-2021 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.SFTPActionConfig;
import io.cdap.plugin.common.SFTPConnector;
import io.cdap.plugin.common.SFTPConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
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
    @Macro
    public String fileNameRegex;

    @Description("Properties that will be used to configure the file destination system.")
    @Nullable
    public String fileSystemProperties;

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

    public Map<String, String> getFileSystemProperties(){
      Map<String, String> properties = new HashMap<>();
      if (fileSystemProperties == null || fileSystemProperties.isEmpty()) {
        return properties;
      }

      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", "=>");
      for (KeyValue<String, String> keyVal : kvParser.parse(fileSystemProperties)) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();
        properties.put(key, val);
      }
      return properties;
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path destination = new Path(config.getDestDirectory());
    Configuration conf = new Configuration();
    Map<String, String> properties = config.getFileSystemProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    FileSystem fileSystem = destination.getFileSystem(conf);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }
    SFTPConnector sftpConnector = null;
    try {
      if (config.getAuthTypeBeingUsed().equals(SFTPConstants.PRIVATE_KEY_SELECT)) {
        sftpConnector = new SFTPConnector(config.getHost(), config.getPort(),
          config.getUserName(), config.getPrivateKey(), config.getPassphrase(), config.getSSHProperties());
      } else {
        sftpConnector = new SFTPConnector(config.getHost(), config.getPort(),
          config.getUserName(), config.getPassword(), config.getSSHProperties());
      }
      copySFTPFiles(fileSystem, destination, sftpConnector, context);
    } catch(Exception e) {
      throw new RuntimeException(String.format("Error occurred while copying files: %s", e.getMessage()), e);
    } finally {
        if (sftpConnector != null) {
          sftpConnector.close();
        }
    }
  }

  /**
   * copySFTPFiles recursively copies all files and subdirectories in a given directory.
   *
   * @param fileSystem The destination file system to copy to
   * @param destination The destination base path to copy to
   * @param connector The SFTP connector to use
   * @param context The context for the action
   * @throws SftpException If any SFTP errors occur
   */
  private void copySFTPFiles(FileSystem fileSystem, Path destination, SFTPConnector connector,
                             ActionContext context) throws SftpException, IOException {
    ChannelSftp channel = connector.getSftpChannel();
    ArrayList<String> filesCopied = new ArrayList<>();
    Vector entries = channel.ls(config.getSrcDirectory());
    for (Object obj : entries) {
      if (!(obj instanceof ChannelSftp.LsEntry)) {
        continue;
      }
      ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) obj;
      recursiveSFTPCopy(channel, fileSystem, Paths.get(config.getSrcDirectory()), destination, filesCopied, entry);
    }
    context.getArguments().set(config.getVariableNameHoldingFileList(), Joiner.on(",").join(filesCopied));
    LOG.info("Variables copied to {}.", Joiner.on(",").join(filesCopied));
  }

  private String getLongPath(java.nio.file.Path prefixPath, String fileName) {
    return prefixPath.resolve(fileName).toString();
  }

  private void recursiveSFTPCopy(ChannelSftp channel, FileSystem fileSystem, java.nio.file.Path prefixPath,
                                 Path destinationPath, List<String> filesCopied, ChannelSftp.LsEntry entry)
    throws SftpException, IOException {
    if (".".equals(entry.getFilename()) || "..".equals(entry.getFilename())) {
      // ignore "." and ".." files
      return;
    }
    // Ignore files that don't match the given file regex
    if (!Strings.isNullOrEmpty(config.fileNameRegex)) {
      if (!entry.getFilename().matches(config.fileNameRegex)) {
        LOG.debug("Skipping file {} since it does not match the regex.", getLongPath(prefixPath, entry.getFilename()));
        return;
      }
    }
    if (entry.getAttrs().isDir()) {
      Path subDirPath = new Path(destinationPath, entry.getFilename());
      if (!fileSystem.exists(subDirPath)) {
        fileSystem.mkdirs(subDirPath);
      }
      Vector entries = channel.ls(getLongPath(prefixPath, entry.getFilename()));
      for (Object obj : entries) {
        if (!(obj instanceof ChannelSftp.LsEntry)) {
          continue;
        }
        ChannelSftp.LsEntry subEntry = (ChannelSftp.LsEntry) obj;
        recursiveSFTPCopy(channel, fileSystem, prefixPath.resolve(entry.getFilename()),
                          new Path(destinationPath, entry.getFilename()), filesCopied, subEntry);
      }
    } else if (entry.getAttrs().isReg()) {
      Path qualifiedDestinationPath = fileSystem.makeQualified(new Path(destinationPath, entry.getFilename()));
      if (config.getExtractZipFiles() && entry.getFilename().endsWith(".zip")) {
        LOG.debug("Downloading zip {} to {}", entry.getFilename(), qualifiedDestinationPath);
        copyJschZip(channel.get(getLongPath(prefixPath, entry.getFilename())), fileSystem, qualifiedDestinationPath);
      } else {
        LOG.debug("Downloading {} to {}", entry.getFilename(), qualifiedDestinationPath.toString());
        try (OutputStream output = fileSystem.create(qualifiedDestinationPath)) {
          InputStream is = channel.get(getLongPath(prefixPath, entry.getFilename()));
          ByteStreams.copy(is, output);
        }
      }
      filesCopied.add(getLongPath(prefixPath, entry.getFilename()));
    }
  }

  private void copyJschZip(InputStream is, FileSystem fs, Path destination) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        LOG.debug("Extracting {}", entry);
        Path destinationPath = fs.makeQualified(new Path(destination, entry.getName()));
        if (entry.isDirectory()) {
          if (!fs.exists(destinationPath)) {
            fs.mkdirs(destinationPath);
          }
        } else {
          try (OutputStream os = fs.create(destinationPath)) {
            LOG.debug("Downloading {} to {}", entry.getName(), destinationPath.toString());
            ByteStreams.copy(zis, os);
          }
        }
      }
    }
  }
}
