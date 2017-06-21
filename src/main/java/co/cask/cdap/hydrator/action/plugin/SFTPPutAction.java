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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.cdap.hydrator.action.common.SFTPActionConfig;
import co.cask.cdap.hydrator.action.common.SFTPConnector;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

    try (SFTPConnector sftp = new SFTPConnector(config.getHost(), config.getPort(), config.getUserName(),
                                                config.getPassword(), config.getSSHProperties())) {
      ChannelSftp channel = sftp.getSftpChannel();

      try {
        channel.mkdir(config.getDestDirectory());
      } catch (SftpException ex) {
        // Suppress since the directory might already exist.
      }

      channel.cd(config.getDestDirectory());

      // Filter out only the files to copy
      FileStatus[] filesToCopy = fileSystem.listStatus(source, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          String fileName = path.getName();
          return fileName.matches(config.getFileNameRegex());
        }
      });

      for (FileStatus file : filesToCopy) {
        Path filePath = file.getPath();
        try (InputStream inputStream = fileSystem.open(filePath)) {
          channel.put(inputStream, filePath.getName());
        }
      }
    }
  }
}
