import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class DistributedReducer {

    private static final String DELIMITER = "$";

    private HashMap<String, ArrayList<String>> mMap;

    private int mPort;

    private ServerSocket mServer;

    private ArrayList<SocketThread> mSocketThreads;

    ArrayList<String> messages;

    private int mNoOfThreads = 0;

    private ReducerImplementation mReducerImpl;

    private String mResult;

    public DistributedReducer(int port) {
        mPort = port;
        init();
    }

    private void init() {
        try {
            mServer = new ServerSocket(mPort);
            mMap = new HashMap<>();
            mSocketThreads = new ArrayList<>();
            mResult = "";
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void startServer() {
        new ServerProcess().start();
    }

    private class ServerProcess implements Runnable {

        Thread thread;

        @Override
        public void run() {
            synchronized (messages) {
                 do {
                    try {
                        Socket socket = mServer.accept();
                        SocketThread socketThread = new SocketThread(socket);
                        socketThread.start();
                        mNoOfThreads++;
                        mSocketThreads.add(socketThread);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } while (mNoOfThreads > 0);
                 postCollection();
            }
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
        }

        @Override
        public void run() {
            String msg;
            while (true) {
                try {
                    msg = mInputStream.readUTF();
                    String values[] = msg.split(DELIMITER);

                    if (Integer.parseInt(values[2]) == Message.MessageType.TYPE_SYSTEM) {
                        if (Integer.parseInt(values[1]) == Message.MessageFlag.END) {
                            mNoOfThreads--;
                            break;
                        }
                    }

                    if (Integer.parseInt(values[2]) == Message.MessageType.TYPE_MAPPER_CHECK) {
                        if (mMap.containsKey(values[1])) {
                            String x = values[0] + DELIMITER +// key
                                    Message.MessageFlag.TYPE_PRESENT + DELIMITER +// flag
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
                            mMap.put(values[0],list);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void postCollection() {
        try {
            DataOutputStream out = new DataOutputStream(new Socket(InetAddress.getLocalHost(),
                    Mapper.mMainPort,
                    InetAddress.getLocalHost(),
                    mPort)
                    .getOutputStream());

            mReducerImpl = new ReducerImplementation();
            Set<String> keys = mMap.keySet();
            Iterator it = keys.iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                ArrayList<String> list = mMap.get(key);
                mReducerImpl.reduce(key,list);
            }
            convertOutputTabularFormat();
            out.writeUTF(mResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void convertOutputTabularFormat() {
        mResult = "";
        Iterator it = mReducerImpl.result.iterator();
        while (it.hasNext()) {
            Message m = (Message) it.next();
            mResult = mResult + m.getKey() + m.getMessage() + "\n";
        }
    }
}
