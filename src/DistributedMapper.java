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

/**
 * functionality of Mapper Server
 */
public class DistributedMapper {

    private static final String DELIMITER = "$";

    private String mFileName;

    private int mPort;

    private int mNoOfReducers;

    private MapperImplementation mMapperImpl;

    MapServer mMapServer;

    Socket[] mReducerSocket;

    ArrayList<String> mCommands;
    ArrayList<String> mReceivedCommands;

    DataInputStream[] inputStreams;
    DataOutputStream[] outputStreams;

    public DistributedMapper(String fileName, int noOfReducers) {
        mFileName = fileName;
        mNoOfReducers = noOfReducers;
        System.out.println("mapper: " + mFileName + " " + mNoOfReducers);
        init();
    }

    private void init() {
        mMapperImpl = new MapperImplementation();
        mCommands = new ArrayList<>();
        mReceivedCommands = new ArrayList<>();
    }

    public void runServer() {
        try {
            Thread.sleep(500);
            mReducerSocket = new Socket[mNoOfReducers];
            inputStreams = new DataInputStream[mNoOfReducers];
            outputStreams = new DataOutputStream[mNoOfReducers];
            mPort = Mapper.mMapperStartingPort;
            while (!isPortAvailable(mPort)) {
                mPort++;
            }
            System.out.println("Starting mapper on port " + mPort);
            System.out.println("Number of reducers " + mNoOfReducers);
            connectSockets();
            messageMainServer("s");
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
        System.out.println("mapper " + mPort + " connecting to sockets");
        while (!check) {
            for (int i = 0; i < mNoOfReducers; i++) {
                try {
                    if (mReducerSocket[i] != null) {
                        continue;
                    }
                    System.out.println("mapper " + mPort + " connecting to socket: " + (Mapper.mReducerStartingPort + i));
                    mReducerSocket[i] = new Socket(InetAddress.getLocalHost(),
                            Mapper.mReducerStartingPort + i);
                    System.out.println("mapper " + mPort + " connected to socket: " + (Mapper.mReducerStartingPort + i));
                    check = true;
                } catch (Exception e) {
                    if (check) {
                        e.printStackTrace();
                    }
                    check = false;
                }
            }
        }
        for (int i = 0; i < mNoOfReducers; i++) {
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
                        Message.MessageType.TYPE_MAPPER_RESULT + DELIMITER +
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
                        System.out.println("command " + command);
                        confirmIndex = -1;
                        for (int i = 0; i < mNoOfReducers; i++) {
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
                            int random = (int)(Math.random() * mNoOfReducers);
                            outputStreams[random].writeUTF(command);
                        }
                    } catch (Exception e) {

                    }
                }
            }

            for (int i = 0; i < mNoOfReducers; i++) {
                try {
                    outputStreams[i].writeUTF("END" +
                            "$" +
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
            DataOutputStream out = new DataOutputStream(new Socket(InetAddress.getLocalHost(), Mapper.mMainPort).getOutputStream());
            out.writeUTF(message + " map " + mPort);
        } catch (Exception e) {

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
            String values[] = splitThroughSeparator(command, DELIMITER);
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
                values = splitThroughSeparator(result, DELIMITER);
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

    /**
     * used for splitting a String providing a separator
     * @param key
     * @param separator
     * @return
     */
    public static String[] splitThroughSeparator(String key, String separator) {
        String[] result;
        ArrayList<String> resultList = new ArrayList<String>();
        int x = key.indexOf(separator);
        while (x > 0) {
            resultList.add(key.subSequence(0,x).toString());
            key = key.subSequence(x + 1,key.length()).toString();
            x = key.indexOf(separator);
        }
        if (!key.equals("") && key != null) {
            resultList.add(key);
        }
        result = new String[resultList.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = resultList.get(i);
        }
        return result;
    }
}
