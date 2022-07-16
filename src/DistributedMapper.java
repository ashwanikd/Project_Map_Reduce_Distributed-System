import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class DistributedMapper {

    private static final String DELIMITER = "$";

    private String mFileName;

    private int mPort;

    private int mNoOfMappers;

    private MapperImplementation mMapperImpl;

    MapServer mMapServer;

    Socket[] mReducerSocket;

    ArrayList<String> mCommands;
    ArrayList<String> mReceivedCommands;

    DataInputStream[] inputStreams;
    DataOutputStream[] outputStreams;

    public DistributedMapper(String fileName, int noOfMappers) {
        mFileName = fileName;
        mNoOfMappers = noOfMappers;
        init();
    }

    private void init() {
        mMapperImpl = new MapperImplementation();
        mMapServer = new MapServer();
        mCommands = new ArrayList<>();
        mReceivedCommands = new ArrayList<>();
    }

    public void runServer() {
        try {
            Thread.sleep(500);
            mReducerSocket = new Socket[mNoOfMappers];
            inputStreams = new DataInputStream[mNoOfMappers];
            outputStreams = new DataOutputStream[mNoOfMappers];
            mPort = Mapper.mMapperStartingPort;
            while (!isPortAvailable(mPort)) {
                mPort++;
            }
            System.out.println("Starting mapper on port " + mPort);
            connectSockets();
            createMapperOutput();
            createMapperCommands();
            startServer();
        } catch (Exception e) {
            System.out.println("Program halted reason: ");
            e.printStackTrace();
        }
    }

    private void connectSockets() {
        boolean check = false;
        while (!check) {
            for (int i = 0; i < mNoOfMappers; i++) {
                try {
                    if (mReducerSocket[i] != null) {
                        mReducerSocket[i] = new Socket(InetAddress.getLocalHost(),
                                Mapper.mReducerStartingPort + i,
                                InetAddress.getLocalHost(),
                                mPort);
                        check = true;
                    }
                    messageMainServer("s");
                } catch (Exception e) {
                    check = false;
                    e.printStackTrace();
                }
            }
        }
        for (int i = 0; i < mNoOfMappers; i++) {
            try {
                inputStreams[i] = new DataInputStream(mReducerSocket[i].getInputStream());
                outputStreams[i] = new DataOutputStream(mReducerSocket[i].getOutputStream());
            } catch (Exception e) {
                check = false;
                e.printStackTrace();
            }
        }
    }

    private void createMapperOutput() {
        File file = new File(mFileName);
        try {
            if (file.exists()) {
                int offSet = 0;
                Scanner scan = new Scanner(file);
                while (scan.hasNextLine()) {
                    Message m = new Message();
                    m.setKey(""+offSet++);
                    m.setMessage(scan.nextLine());
                    m.setType(Message.MessageType.TYPE_MAPPER_RESULT);
                    mMapperImpl.map(m);
                }
            } else {
                System.out.println("File not exists" +
                        "exiting the program");
                System.exit(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createMapperCommands() {
        String command;
        for (int i = 0; i < mMapperImpl.mMessageList.getMessages().size() ; i++) {
            Message m = mMapperImpl.mMessageList.getMessages().get(i);
            command = m.getKey() + DELIMITER +
                        m.getFlag() + DELIMITER +
                        m.getType() + DELIMITER +
                        m.getMessage();
            mCommands.add(command);
        }
    }

    private void startServer() {
        mMapServer = new MapServer();
        mMapServer.start();
    }

    private class MapServer implements Runnable {

        Thread thread;

        MapServer() {
            thread = new Thread(this,"Mapping server " + mPort);
        }

        public void start() {
            thread.start();
        }

        @Override
        public void run() {
            String command;
            int confirmIndex = 0;
            synchronized (mCommands) {
                while (mCommands.size() > 0) {
                    try {
                        command = mCommands.remove(0);
                        confirmIndex = -1;
                        for (int i = 0; i < mNoOfMappers; i++) {
                            CheckKeyPresentTask task = new CheckKeyPresentTask(i, command);
                            Thread t = new Thread(task,"new task");
                            t.start();
                            t.join();
                            if (task.isPresent()) {
                                confirmIndex = i;
                                break;
                            }
                        }
                        if (confirmIndex != -1) {
                            outputStreams[confirmIndex].writeUTF(command);
                        } else {
                            int random = (int)(Math.random() * mNoOfMappers);
                            outputStreams[random].writeUTF(command);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            for (int i = 0; i < mNoOfMappers; i++) {
                try {
                    outputStreams[i].writeUTF("END$" +
                            Message.MessageFlag.END +
                            "$" +
                            Message.MessageType.TYPE_SYSTEM +
                            "$" +
                            "end");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            messageMainServer("c");
        }
    }

    private void messageMainServer(String message) {
        try {
            DataOutputStream out = new DataOutputStream(new Socket(InetAddress.getLocalHost(),
                    Mapper.mMainPort,
                    InetAddress.getLocalHost(),
                    mPort)
                    .getOutputStream());
            out.writeUTF(message + " map " + mPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class CheckKeyPresentTask implements Runnable {
        private int reducerIndex;
        String command;

        private boolean present;

        CheckKeyPresentTask(int reducerIndex, String command) {
            this.reducerIndex = reducerIndex;
            this.command = command;
            present = false;
        }
        @Override
        public void run() {
            String values[] = command.split(DELIMITER);
            String x = values[0] + DELIMITER +// key
                    Message.MessageFlag.TYPE_PRESENT + DELIMITER +// flag
                    Message.MessageType.TYPE_MAPPER_CHECK + DELIMITER +// type
                    values[3]; // message
            try {
                outputStreams[reducerIndex].writeUTF(x);
                String result = null;
                while (result == null) {
                    result = inputStreams[reducerIndex].readUTF();
                }
                values = result.split(DELIMITER);
                if (Integer.parseInt(values[1]) == Message.MessageFlag.TYPE_PRESENT) {
                    present = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public boolean isPresent() {
            return present;
        }
    }

    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port to check for availability
     */
    public static boolean isPortAvailable(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }
        return false;
    }
}
