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
import java.nio.charset.StandardCharsets;
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
    public SCPRemotetoRemoteAction(SCPRemotetoRemoteActionConfig config) { this.config = config; }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);
        LOG.debug("Executing the 'run' method of the {} plugin", PLUGIN_NAME);
        config.validate();
    }

    @Override
    public void run(ActionContext context) throws JSchException, IOException {
        LOG.debug("Running the 'run' method of the {} plugin.", PLUGIN_NAME);
        config.validate();
        byte[] key = config.getPrivateKey();
        byte[] passphrase = config.getPassphrase();
        JSch jsch = new JSch();
        jsch.addIdentity("key", key, null, passphrase);
        Session session = jsch.getSession(config.getUserNameBastion(),
            config.getHostBastion(), config.getPort());
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        ChannelExec channel = (ChannelExec) session.openChannel("exec");
        String userA = config.getUserNameA();
        String hostA = config.getHostA();
        String source = config.getSource();
        String pathSource = userA + "@" + hostA + ":" + source;
        String userB = config.getUserNameB();
        String hostB = config.getHostB();
        String dest = config.getDest();
        String pathDest = userB + "@" + hostB + ":" + dest;
        String dirFlag = config.getDirFlag();
        if (dirFlag.equals("directory-off")){
            dirFlag = "";
        } else {
            dirFlag = "-r";
        }
        String compressFlag = config.getCompressFlag();
        if (compressFlag.equals("compression-off")){
            compressFlag = "";
        } else {
            compressFlag = "-C";
        }
        String verboseFlag = config.getVerboseFlag();
        if (verboseFlag.equals("verbose-off")){
            verboseFlag = "";
        } else {
            verboseFlag = "-v";
        }
        //Host A -> Host B
        channel.setCommand("scp " + compressFlag + " " + verboseFlag +
            " " + dirFlag + " " + pathSource + " " + pathDest);
        channel.connect();
        verboseLogging(channel);
        channel.disconnect();
        session.disconnect();
    }

    private void verboseLogging(ChannelExec channel) throws IOException {
        StringBuilder inputBuffer = new StringBuilder();
        StringBuilder outputBuffer = new StringBuilder();
        InputStream in = channel.getInputStream();
        InputStream out = channel.getExtInputStream();
        byte[] tmp = new byte[1024];
        int lenIn = in.read(tmp, 0, tmp.length);
        while (lenIn > 0){
            inputBuffer.append(new String(tmp, 0, lenIn, StandardCharsets.UTF_8));
            lenIn = in.read(tmp, 0, tmp.length);
        }
        int lenOut = out.read(tmp, 0, tmp.length);
        while (lenOut > 0){
            outputBuffer.append(new String(tmp, 0, lenOut, StandardCharsets.UTF_8));
            lenOut = out.read(tmp, 0, tmp.length);
        }
        if (channel.isClosed()) {
            LOG.info("Exit-Status: " + channel.getExitStatus());
        }
        LOG.info("Input: " + inputBuffer.toString());
        LOG.info("Verbose Info: " + outputBuffer.toString());
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

        @Description("Name of the user used to login to SSH server that contains the files to copy.")
        @Macro
        public String userNameA;

        @Description("Hostname or IP Address of the SSH server that contains the files to copy.")
        @Macro
        public String hostA;

        @Description("Absolute path on Host A to copy")
        @Macro
        public String sourcePath;

        @Description("Name of the user used to login to SSH server that files should be copied to.")
        @Macro
        public String userNameB;

        @Description("Hostname or IP Address of the SSH server that the files should be copied to.")
        @Macro
        public String hostB;

        @Description("Location files should be copied to")
        @Macro
        public String destPath;

        @Nullable
        @Name("compressionFlag")
        @Description("Setting Compression Flag")
        public String compressFlag;

        @Nullable
        @Name("verboseFlag")
        @Description("Setting Verbose Flag for more Log data")
        public String verboseFlag;

        @Nullable
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

        public byte[] getPrivateKey() { return privateKey.getBytes(StandardCharsets.UTF_8); }

        public byte[] getPassphrase(){
           return passphrase == null ? new byte[0] : passphrase.getBytes(StandardCharsets.UTF_8); }

        public String getUserNameA() { return userNameA; }

        public String getHostA() { return hostA; }

        public String getSource() { return sourcePath; }

        public String getUserNameB() { return userNameB; }

        public String getHostB() { return hostB; }

        public String getDest() { return destPath; }

        public String getCompressFlag() { return compressFlag; }

        public String getVerboseFlag() { return verboseFlag; }

        public String getDirFlag() { return dirFlag; }

        public void validate() throws IllegalArgumentException {
            // The containsMacro function can be used to check if there is a macro in the config option.
            // At runtime, the containsMacro function will always return false.
        }
    }
}