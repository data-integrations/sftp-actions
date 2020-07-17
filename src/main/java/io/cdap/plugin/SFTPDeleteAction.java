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
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.common.SFTPActionConfig;
import io.cdap.plugin.common.SFTPConnector;
import com.jcraft.jsch.ChannelSftp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link Action} to delete files on the SFTP server.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SFTPDelete")
public class SFTPDeleteAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(SFTPDeleteAction.class);
  private SFTPDeleteActionConfig config;
  public SFTPDeleteAction(SFTPDeleteActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  public static class SFTPDeleteActionConfig extends SFTPActionConfig {
    @Description("Comma separated list of files to be deleted from FTP server.")
    @Macro
    public String filesToDelete;

    @Description("Boolean flag to determine if execution should continue if there is an error while deleting any file." +
            " Defaults to 'false'.")
    boolean continueOnError;

    public String getFilesToDelete() {
      return filesToDelete;
    }

    public SFTPDeleteActionConfig(String host, int port, String userName, String password,
                                String sshProperties, String filesToDelete, String authType){
      this.host = host;
      this.port = port;
      this.userName = userName;
      this.password = password;
      this.sshProperties = sshProperties;
      this.filesToDelete = filesToDelete;
      this.authTypeBeingUsed = authType;
    }

    public void validate() throws IllegalArgumentException {
      // Check for required parameters
      // Check for required params for each action
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    String filesToDelete = config.getFilesToDelete();
    if (filesToDelete == null || filesToDelete.isEmpty()) {
      return;
    }
    if (config.getAuthTypeBeingUsed().equals("privatekey-select")) {
      try (SFTPConnector SFTPConnector = new SFTPConnector(config.getHost(), config.getPort(), config.getUserName(),
              config.getPrivateKey(), config.getPassphrase(), config.getSSHProperties())) {
        sftpDeleteLogic(filesToDelete, SFTPConnector);
      } catch (Exception e){
        LOG.error(String.valueOf(e));
      }
    } else {
      try (SFTPConnector SFTPConnector = new SFTPConnector(config.getHost(), config.getPort(), config.getUserName(),
              config.getPassword(), config.getSSHProperties())) {
        sftpDeleteLogic(filesToDelete, SFTPConnector);
      } catch (Exception e){
        LOG.error(String.valueOf(e));
      }
    }
  }

    private void sftpDeleteLogic (String filesToDelete, SFTPConnector SFTPConnector) throws SftpException {
      ChannelSftp channelSftp = SFTPConnector.getSftpChannel();
      for (String fileToDelete : filesToDelete.split(",")) {
        LOG.info("Deleting {}", fileToDelete);
        try {
          channelSftp.rm(fileToDelete);
        } catch (Throwable t) {
          if (config.continueOnError) {
            LOG.warn("Error deleting file {}.", fileToDelete, t);
          } else {
            throw t;
          }
        }
      }
    }
  }