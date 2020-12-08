/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.jcraft.jsch.SftpATTRS;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.common.SFTPActionConfig;
import io.cdap.plugin.common.SFTPConnector;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import io.cdap.plugin.common.SFTPConstants;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * An {@link Action} that will copy files to SFTP server from a source directory.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SFTPPut")
public class SFTPPutAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(SFTPPutAction.class);

  private SFTPPutActionConfig config;

  /**
   * Configurations for the SFTP put action plugin.
   */
  public class SFTPPutActionConfig extends SFTPActionConfig {

    @Description("Directory or File on the Filesystem which needs to be copied to the SFTP Server.")
    @Macro
    public String srcPath;

    @Description("Destination directory on the SFTP server. If the directory does not exist, it will be created.")
    @Macro
    public String destDirectory;

    @Description("Regex to copy only the file names that match. By default, all files will be copied.")
    @Nullable
    @Macro
    public String fileNameRegex;

    public String getSrcPath() {
      return srcPath;
    }

    public String getDestDirectory() {
      return destDirectory;
    }

    public String getFileNameRegex() {
      return (fileNameRegex != null) ? fileNameRegex : ".*";
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path source = new Path(config.getSrcPath());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    if (!fileSystem.exists(source)) {
      throw new RuntimeException(String.format("Source Path doesn't exist at %s", source));
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
      putSFTPFiles(fileSystem, source, sftpConnector);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error occurred while connecting to SFTP Server %s %s", e.getMessage(), e));
    } finally {
      if (sftpConnector != null) {
        sftpConnector.close();
      }
    }
  }

  private void putSFTPFiles(FileSystem fileSystem, Path source, SFTPConnector sftp)
    throws SftpException, IOException {
    ChannelSftp channel = sftp.getSftpChannel();
    SftpATTRS attrs = null;

    try {
      attrs = channel.stat(config.getDestDirectory());
    } catch (Exception e) {
      LOG.debug(String.format("Directory does not exist %s %s", e.getMessage(), e));
    }
    if (attrs != null) {
      LOG.info(String.format("Directory exist %s", attrs.isDir()));
    } else {
      LOG.info(String.format("Creating Directory %s", config.getDestDirectory()));
      channel.mkdir(config.getDestDirectory());
    }
    channel.cd(config.getDestDirectory());
    // Filter out only the files to copy
    FileStatus[] filesToCopy = fileSystem.listStatus(source, path -> {
      String fileName = path.getName();
      return fileName.matches(config.getFileNameRegex());
    });
    for (FileStatus file : filesToCopy) {
      Path filePath = file.getPath();
      try (InputStream inputStream = fileSystem.open(filePath)) {
        channel.put(inputStream, filePath.getName());
      }
    }
  }
}
