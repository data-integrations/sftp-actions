package io.cdap.plugin;

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

import com.google.common.annotations.VisibleForTesting;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

import com.jcraft.jsch.Session;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;


@Plugin(type = Action.PLUGIN_TYPE)
@Name(SCPRemotetoRemoteAction.PLUGIN_NAME)
@Description("This action will connect to a Bastion Host and execute a SCP command to copy a file from Host A to Host B.")
public class SCPRemotetoRemoteAction extends Action {

    public static final String PLUGIN_NAME = "SCPRemote";
    private static final Logger LOG = LoggerFactory.getLogger(SCPRemotetoRemoteAction.class);

    private final SCPRemotetoRemoteActionConfig config;

    @VisibleForTesting
    public SCPRemotetoRemoteAction(SCPRemotetoRemoteActionConfig config) {
        this.config = config;
    }

    /**
     * This function is executed by the Pipelines framework when the Pipeline is deployed. This
     * is a good place to validate any configuration options the user has entered. If this throws
     * an exception, the Pipeline will not be deployed and the user will be shown the error message.
     */
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);
        LOG.debug(String.format("Running the 'configurePipeline' method of the %s plugin.", PLUGIN_NAME));
        config.validate();
    }

    @Override
    public void run(ActionContext context) throws JSchException, IOException {
        LOG.debug(String.format("Running the 'run' method of the %s plugin.", PLUGIN_NAME));
        config.validate();

        byte[] key = config.getPrivateKey();
        byte[] passphrase = config.getPassphrase();
        JSch jsch = new JSch();
        jsch.addIdentity("key", key, null,passphrase);
        Session session = jsch.getSession(config.getUserNameBastion(),config.getHostBastion(),config.getPort());

        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        ChannelExec channel = (ChannelExec) session.openChannel("exec");

        String userA = config.getUserNameA();
        String hostA = config.getHostA();
        String source = config.getSource();
        String pathSource = userA+"@"+hostA+":"+source;

        String userB = config.getUserNameB();
        String hostB = config.getHostB();
        String dest= config.getDest();
        String pathDest = userB+"@"+hostB+":"+dest;

        String dirFlag = config.getDirFlag();
        String compressFlag = config.getCompressFlag();
        String verboseFlag = config.getVerboseFlag();

        //Host A -> Host B
        channel.setCommand("scp "+compressFlag+ " "+verboseFlag+
            " " +dirFlag +" " +pathSource +" " +pathDest);

        StringBuilder outputBuffer = new StringBuilder();
        StringBuilder errorBuffer = new StringBuilder();

        InputStream in = channel.getInputStream();
        InputStream err = channel.getExtInputStream();

        channel.connect();

        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
                outputBuffer.append(new String(tmp, 0, i));
            }
            while (err.available() > 0) {
                int i = err.read(tmp, 0, 1024);
                if (i < 0) break;
                errorBuffer.append(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                if ((in.available() > 0) || (err.available() > 0)) continue;
                System.out.println("exit-status: " + channel.getExitStatus());
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
        }

        LOG.debug("output: " + outputBuffer.toString());
        LOG.info("info: " + errorBuffer.toString());

        channel.disconnect();
        session.disconnect();

    }

    /**
     * The config class for {@link SCPRemotetoRemoteAction} that contains all properties that need to be filled in by
     * the user when building a Pipeline.
     */
    public static class SCPRemotetoRemoteActionConfig extends PluginConfig {

        @Description("Hostname or IP Address of the SSH server.")
        @Macro
        public String hostBastion;

        @Description("Port on which SSH server is running. Defaults to 22.")
        @Nullable
        @Macro
        public Integer port;

        @Description("Name of the User used to login to the Bastion SSH server.")
        @Macro
        public String userNameBastion;

        @Description("Private Key to be used to login to the Bastion SSH Server. SSH key must be of RSA type")
        @Macro
        public String privateKey;

        @Description("Passphrase to be used with private key if passphrase was enabled when key was created. ")
        @Macro
        @Nullable
        public String passphrase;

        @Description("Name of the user used to login to SSH server.")
        @Macro
        public String userNameA;

        @Description("Hostname or IP Address of the SSH server that contains the files to copy.")
        @Macro
        public String hostA;

        @Description("Absolute path on Host A")
        @Macro
        public String sourcePath;

        @Description("Name of the user used to login to SSH server.")
        @Macro
        public String userNameB;

        @Description("Hostname or IP Address of the SSH server that the files should be copied to.")
        @Macro
        public String hostB;

        @Description("Location files should be copied to")
        @Macro
        public String destPath;

        @Name("compressionFlag")
        @Description("Setting Compression Flag")
        public String compressFlag;

        @Name("verboseFlag")
        @Description("Setting Verbose Flag for more Log data")
        public String verboseFlag;

        @Name("directoryFlag")
        @Description("Setting Directory Flag")
        public String dirFlag;



        public String getHostBastion() {
            return hostBastion;
        }

        public int getPort() {
            return (port != null) ? port : 22;
        }

        public String getUserNameBastion() {
            return userNameBastion;
        }

        public byte[] getPrivateKey() {
            assert privateKey != null;
            return privateKey.getBytes();
        }

        public byte[] getPassphrase(){
            if (passphrase == null){
                passphrase = "";
            }
            return passphrase.getBytes();
        }


        public String getUserNameA() {
            return userNameA;
        }

        public String getHostA() {
            return hostA;
        }

        public String getSource() {
            return sourcePath;
        }


        public String getUserNameB() {
            return userNameB;
        }

        public String getHostB() {
            return hostB;
        }


        public String getDest() {
            return destPath;
        }

        public String getCompressFlag() {
            if (compressFlag.equals("compression-off")){
                return compressFlag = "";
            }
            return compressFlag="-C";
        }

        public String getVerboseFlag() {
            if (verboseFlag.equals("verbose-off")){
                return verboseFlag = "";
            }
            return verboseFlag="-v";
        }

        public String getDirFlag() {
            if (dirFlag.equals("directory-off")){
                return dirFlag = "";
            }
            return dirFlag="-r";
        }

        /**
         * You can leverage this function to validate the configure options entered by the user.
         */

        public void validate() throws IllegalArgumentException {
            // The containsMacro function can be used to check if there is a macro in the config option.
            // At runtime, the containsMacro function will always return false.

        }
    }
}


