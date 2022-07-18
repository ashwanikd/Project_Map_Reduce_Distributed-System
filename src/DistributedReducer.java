import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * functionality of Separator Server
 */
public class DistributedReducer {

    private static final String DELIMITER = "$";

    private HashMap<String, ArrayList<String>> mMap;

    private int mPort;

    private ServerSocket mServer;

    private ArrayList<SocketThread> mSocketThreads;

    private int mNoOfThreads = 0;

    private ReducerImplementation mReducerImpl;

    private String mResult;

    private ServerProcess mServerProcess;

    public DistributedReducer() {
        mPort = Reducer.mReducerStartingPort;
        init();
    }

    private void init() {
        try {
            while (!isPortAvailable(mPort)) {
                mPort++;
            }
            mServer = new ServerSocket(mPort);
            mServer.setSoTimeout(2000);
            System.out.println("starting reducer on server " + mPort);
            mMap = new HashMap<>();
            mSocketThreads = new ArrayList<>();
            mResult = "";
        } catch (Exception e) {

        }
    }

    public void startServer() {
        mServerProcess = new ServerProcess();
        mServerProcess.start();
        messageMainServer("s");
    }

    private void messageMainServer(String message) {
        try {
            DataOutputStream out = new DataOutputStream(new Socket(InetAddress.getLocalHost(), Mapper.mMainPort).getOutputStream());
            out.writeUTF(message + " reduce " + mPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class ServerProcess implements Runnable {

        Thread thread;

        @Override
        public void run() {
            try {
                do {
                    System.out.println("Server listening");
                    Socket socket = mServer.accept();
                    System.out.println("connected to " + socket.getPort());
                    SocketThread socketThread = new SocketThread(socket);
                    socketThread.start();
                    mNoOfThreads++;
                    mSocketThreads.add(socketThread);
                } while (mNoOfThreads > 0);
            } catch (Exception e) {
                e.printStackTrace();
            }
            postCollection();
        }

        public void start() {
            thread = new Thread(this, "Reducer server");
            thread.start();
        }
    }

    private class SocketThread implements Runnable {

        Thread thread;

        Socket mSocket;

        DataInputStream mInputStream;

        DataOutputStream mOutputStream;

        SocketThread(Socket socket) {
            mSocket = socket;
            try {
                mInputStream = new DataInputStream(mSocket.getInputStream());
                mOutputStream = new DataOutputStream(mSocket.getOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void start() {
            thread = new Thread(this,"socket for mapper");
            thread.start();
        }

        @Override
        public void run() {
            String msg;
            synchronized (mMap) {
                while (true) {
                    try {
                        msg = mInputStream.readUTF();

                        String values[] = splitThroughSeparator(msg, DELIMITER);

                        System.out.println("message from " + mSocket.getPort() + " " + msg);

                        for (String key : mMap.keySet()) {
                            System.out.println("key: " + key);
                            ArrayList<String> list = mMap.get(key);
                            for (int i = 0; i < list.size(); i++) {
                                System.out.println(list.get(i));
                            }
                        }

                        if (Integer.parseInt(values[2]) == Message.MessageType.TYPE_SYSTEM) {
                            if (Integer.parseInt(values[1]) == Message.MessageFlag.END) {
                                mNoOfThreads--;
                                System.out.println("no of threads: " + mNoOfThreads);
                                thread.stop();
                            }
                        }

                        if (Integer.parseInt(values[2]) == Message.MessageType.TYPE_MAPPER_CHECK) {
                            if (mMap.containsKey(values[0])) {
                                String x = values[0] + DELIMITER +// key
                                        Message.MessageFlag.TYPE_PRESENT + DELIMITER +// flag
                                        Message.MessageType.TYPE_MAPPER_CHECK + DELIMITER +// type
                                        values[3]; // message
                                mOutputStream.writeUTF(x);
                            } else {
                                String x = values[0] + DELIMITER +// key
                                        Message.MessageFlag.TYPE_ABSENT + DELIMITER +// flag
                                        Message.MessageType.TYPE_MAPPER_CHECK + DELIMITER +// type
                                        values[3]; // message
                                mOutputStream.writeUTF(x);
                            }
                        } else if (Integer.parseInt(values[2]) == Message.MessageType.TYPE_MAPPER_RESULT) {
                            if (mMap.containsKey(values[0])) {
                                mMap.get(values[0]).add(values[3]);
                            } else {
                                ArrayList<String> list = new ArrayList<>();
                                list.add(values[3]);
                                mMap.put(values[0], list);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        }
    }

    private void postCollection() {
        synchronized (mMap) {
            try {
                mReducerImpl = new ReducerImplementation();
                System.out.println("postCollection...");
                for (String key : mMap.keySet()) {
                    System.out.println("key: " + key);
                    ArrayList<String> list = mMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        System.out.println(list.get(i));
                    }
                    mReducerImpl.reduce(key, list);
                }
                convertOutputTabularFormat();
                DataOutputStream out = new DataOutputStream(new Socket(InetAddress.getLocalHost(), Mapper.mMainPort).getOutputStream());
                out.writeUTF(mResult);
                messageMainServer("c");
                System.exit(0);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void convertOutputTabularFormat() {
        mResult = "";
        Iterator it = mReducerImpl.result.iterator();
        while (it.hasNext()) {
            Message m = (Message) it.next();
            mResult = mResult + m.getKey() + "," + m.getMessage() + "\n";
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
