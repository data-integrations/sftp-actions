package io.cdap.plugin;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import io.cdap.cdap.etl.mock.action.MockActionContext;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.After;
import org.junit.Before;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.sham.sftp.MockSftpServer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;



public class SFTPCopyActionTest {

MockSftpServer server;
    Session sshSession;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
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
        File tempFile = tempFolder.newFile();
        Files.write(tempFile.toPath(), "test".getBytes(StandardCharsets.UTF_8));

        String sourcePath = tempFile.getAbsoluteFile().toString();
        String destPath = server.getBaseDirectory().toString();
        SFTPCopyAction.SFTPCopyActionConfig config = new SFTPCopyAction.SFTPCopyActionConfig(
                "localhost",
                9022,
                "tester",
                "testing",
                "",
                sourcePath,
                destPath,
                "password");
        MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
        new SFTPCopyAction(config).configurePipeline(configurer);
        new SFTPCopyAction(config).run(new MockActionContext());
    }

}





