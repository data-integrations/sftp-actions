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



public class SFTPDeleteTest {

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
        SFTPDeleteAction.SFTPDeleteActionConfig config = new SFTPDeleteAction.SFTPDeleteActionConfig(
                    "localhost",
                    9022,
                    "tester",
                    "testing",
                    "",
                    "",
                    "password");
        MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
        new SFTPDeleteAction(config).configurePipeline(configurer);
        new SFTPDeleteAction(config).run(new MockActionContext());
    }

}





