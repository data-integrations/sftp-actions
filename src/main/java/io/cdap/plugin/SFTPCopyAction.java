/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.jcraft.jsch.SftpException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.common.SFTPActionConfig;
import io.cdap.plugin.common.SFTPConnector;
import io.cdap.plugin.common.KeyValueListParser;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.ChannelSftp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }
  /**
   * Configurations for the FTP copy action plugin.
   */
  public static class SFTPCopyActionConfig extends SFTPActionConfig {
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

    public SFTPCopyActionConfig(String host, int port, String userName, String password,
        String sshProperties, String srcPath, String destDirectory, String authType){
      this.host = host;
      this.port = port;
      this.userName = userName;
      this.password = password;
      this.sshProperties = sshProperties;
      this.srcDirectory = srcPath;
      this.destDirectory = destDirectory;
      this.authTypeBeingUsed = authType;
    }

    public void validate() throws IllegalArgumentException {
      // Check for required parameters
      // Check for required params for each action
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
    FileSystem fileSystem = FileSystem.get(conf);
    destination = fileSystem.makeQualified(destination);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }
    if (config.getAuthTypeBeingUsed().equals("privatekey-select")) {
      try (SFTPConnector SFTPConnector = new SFTPConnector(config.getHost(), config.getPort(),
          config.getUserName(), config.getPrivateKey(), config.getPassphrase(), config.getSSHProperties())) {
        sftpCopyLogic(fileSystem, destination, SFTPConnector, context);
      } catch (Exception e){
        LOG.error(String.valueOf(e));
      }
    } else {
      try (SFTPConnector SFTPConnector = new SFTPConnector(config.getHost(), config.getPort(),
          config.getUserName(), config.getPassword(), config.getSSHProperties())) {
        sftpCopyLogic(fileSystem, destination, SFTPConnector, context);
      } catch (Exception e) {
        LOG.error(String.valueOf(e));
      }
    }
  }

  public void sftpCopyLogic(FileSystem fileSystem, Path destination, SFTPConnector SFTPConnector,
      ActionContext context) throws SftpException, IOException {
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
        }
        filesCopied.add(completeFileName);
      }
      context.getArguments().set(config.getVariableNameHoldingFileList(), Joiner.on(",").join(filesCopied));
      LOG.info("Variables copied to {}.", Joiner.on(",").join(filesCopied));
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