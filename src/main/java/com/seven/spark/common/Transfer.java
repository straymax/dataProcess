package com.seven.spark.common;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    straymax@163.com
 * date     2018/10/23 上午11:04
 * <p>
 * 文件上传
 * 远程调用命令
 */
public class Transfer {

    private static final Logger LOG = LoggerFactory.getLogger(Transfer.class);
    //地址
    private String host;
    //用户名
    private String user;
    //密码
    private String password;
    //端口
    private int port;
    //会话
    private Session session;

    /**
     * 创建一个连接
     *
     * @param host     地址
     * @param user     用户名
     * @param password 密码
     * @param port     ssh端口
     */
    public Transfer(String host, String user, String password, int port) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.port = port;
    }

    private void initialSession() throws Exception {
        if (session == null) {
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, port);
            session.setUserInfo(new UserInfo() {

                public String getPassphrase() {
                    return null;
                }

                public String getPassword() {
                    return null;
                }

                public boolean promptPassword(String arg0) {
                    return false;
                }

                public boolean promptPassphrase(String arg0) {
                    return false;
                }

                public boolean promptYesNo(String arg0) {
                    return true;
                }

                public void showMessage(String arg0) {
                }

            });
            session.setPassword(password);
            session.connect();
        }
    }

    /**
     * 关闭连接
     *
     * @throws Exception
     */
    public void close() throws Exception {
        if (session != null && session.isConnected()) {
            session.disconnect();
            session = null;
        }
        LOG.info("session close is success");
    }

    /**
     * 上传文件
     *
     * @param localPath  本地路径，若为空，表示当前路径
     * @param localFile  本地文件名，若为空或是“*”，表示目前下全部文件
     * @param remotePath 远程路径，若为空，表示当前路径，若服务器上无此目录，则会自动创建
     * @throws Exception
     */
    public void uploadFile(String localPath, String localFile, String remotePath)
            throws Exception {
        this.initialSession();
        Channel channelSftp = session.openChannel("sftp");
        channelSftp.connect();
        ChannelSftp c = (ChannelSftp) channelSftp;
        String remoteFile = null;
        if (remotePath != null && remotePath.trim().length() > 0) {
            try {
                c.mkdir(remotePath);
            } catch (Exception e) {
            }
            remoteFile = remotePath + "/.";
        } else {
            remoteFile = ".";
        }
        String file = null;
        if (localFile == null || localFile.trim().length() == 0) {
            file = "*";
        } else {
            file = localFile;
        }
        if (localPath != null && localPath.trim().length() > 0) {
            if (localPath.endsWith("/")) {
                file = localPath + file;
            } else {
                file = localPath + "/" + file;
            }
        }
        c.put(file, remoteFile);

        channelSftp.disconnect();
    }


    /**
     * 调用命令
     *
     * @param command 需要执行的命令
     * @return        返回执行结果
     * @throws Exception
     */
    public String runCommand(String command) throws Exception {

        LOG.info("[" + command + "] begin", host, user);

        this.initialSession();
        InputStream in = null;
        InputStream err = null;
        BufferedReader inReader = null;
        BufferedReader errReader = null;
        int time = 0;
        String s = null;
        boolean run = false;
        StringBuffer sb = new StringBuffer();

        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);
        channel.setInputStream(null);
        ((ChannelExec) channel).setErrStream(null);
        err = ((ChannelExec) channel).getErrStream();
        in = channel.getInputStream();
        channel.connect();
        inReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        errReader = new BufferedReader(new InputStreamReader(err, "UTF-8"));

        while (true) {
            s = errReader.readLine();
            if (s != null) {
                sb.append("error:" + s).append("\n");
            } else {
                run = true;
                break;
            }
        }
        while (true) {
            s = inReader.readLine();
            if (s != null) {
                sb.append("info:" + s).append("\n");
            } else {
                run = true;
                break;
            }
        }

        while (true) {
            if (channel.isClosed() || run) {
                LOG.info("[" + command + "] finish: " + channel.getExitStatus(), host, user);
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ee) {
            }
            if (time > 180) {
                LOG.info("[" + command + "] finish2: " + channel.getExitStatus(), host, user);
                break;
            }
            time++;
        }

        inReader.close();
        errReader.close();
        channel.disconnect();
        session.disconnect();
        LOG.info(sb.toString());
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        Transfer transfer = new Transfer("127.0.0.1", "root", "123456", 22);
        transfer.uploadFile("/Users/seven/shell/max/", "upload.sh", "/Users/seven/shell/");
        //若无需执行command命令，则关闭连接
        //transfer.close();
        System.out.println(transfer.runCommand("sh /Users/seven/record.sh"));
    }
}
