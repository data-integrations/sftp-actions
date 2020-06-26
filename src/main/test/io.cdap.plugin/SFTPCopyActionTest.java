package io.cdap.plugin;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import io.cdap.cdap.etl.mock.action.MockActionContext;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.After;
import org.junit.Before;


import org.junit.Test;
import software.sham.sftp.MockSftpServer;

import java.io.IOException;
import java.util.Properties;



public class SFTPCopyActionTest {

MockSftpServer server;
    Session sshSession;

    @Before
    public void initSftp() throws IOException {
        server = new MockSftpServer(9022);
    }

    @Before
    public void initSshClient() throws JSchException {
        JSch jsch = new JSch();
        sshSession = jsch.getSession("tester", "localhost", 9022);
        Properties config = new Properties();
        config.setProperty("StrictHostKeyChecking", "no");
        sshSession.setConfig(config);
        sshSession.setPassword("testing");
        sshSession.connect();
    }

    @After
    public void stopSftp() throws IOException {
        server.stop();
    }

    @Test
    public void testCopyFile() throws Exception {
        String filePath = "src/main/test/resources/";
        String destPath = server.getBaseDirectory().toString();
        SFTPCopyAction.SFTPCopyActionConfig config = new SFTPCopyAction.SFTPCopyActionConfig(
                "localhost",
                9022,
                "tester",
                "testing",
                "",
                filePath,
                destPath,
                "password");
        MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
        new SFTPCopyAction(config).configurePipeline(configurer);
        new SFTPCopyAction(config).run(new MockActionContext());
    }

}





