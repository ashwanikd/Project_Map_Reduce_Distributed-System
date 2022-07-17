import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MainServer {

    private int mPort;

    private ServerSocket mServer;

    private File mFile;

    private FileWriter writer;

    private HashMap<Integer,Integer> mappers;
    private HashMap<Integer,Integer> reducers;

    private boolean check = false;

    public MainServer(int port) {
        mPort = port;
        init();
    }

    private void init() {
        mFile = new File("output.csv");
        if (mFile.exists()) {
            mFile.delete();
        }
        try {
            if (mFile.createNewFile()) {
                System.out.println("Successfully created file: " + mFile.getName());
                writer = new FileWriter(mFile,true);
            }
            mappers = new HashMap<>();
            reducers = new HashMap<>();
            mServer = new ServerSocket(mPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startServer() {
        new MainServerThread().start();
    }

    private class MainServerThread implements Runnable {

        Thread thread;

        @Override
        public void run() {
            while (true) {
                try {
                    do {
                        Socket socket = mServer.accept();
                        new SocketThread(socket).start();
                    } while (allFinished());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void start() {
            thread = new Thread(this,"main server");
            thread.start();
        }
    }

    private class SocketThread implements Runnable {

        private Socket mSocket;

        private DataInputStream mInputStream;

        Thread thread;

        SocketThread(Socket socket) {
            mSocket = socket;
            try {
                mInputStream = new DataInputStream(mSocket.getInputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void start() {
            thread = new Thread(this, "socket thread");
            thread.start();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String message = mInputStream.readUTF();
                    System.out.println("recieved message :" + message);
                    if (message != null) {
                        String[] values = splitThroughSeparator(message," ");
                        if (values[0].equals("s")) {
                            int x = Integer.parseInt(values[2]);
                            if (values[1].equals("map")) {
                                if (mappers.containsKey(x)) {
                                    mappers.put(x, mappers.get(x) + 1);
                                } else {
                                    mappers.put(x, mappers.get(x) + 1);
                                }
                            } else if (values[1].equals("reduce")) {
                                if (reducers.containsKey(x)) {
                                    reducers.put(x, reducers.get(x) + 1);
                                } else {
                                    reducers.put(x, reducers.get(x) + 1);
                                }
                            }
                        } else if (values[0].equals("c")) {
                            int x = Integer.parseInt(values[2]);
                            if (values[1].equals("map")) {
                                mappers.put(x,mappers.get(x) - 1);
                            } else if (values[1].equals("reduce")) {
                                reducers.put(x,reducers.get(x) - 1);
                            }
                        } else {
                            if (!check) {
                                writer.write(message);
                                check = true;
                            } else {
                                writer.write("\n" + message);
                            }
                        }
                    }
                } catch (Exception e) {

                }
            }
        }
    }

    private boolean allFinished() {
        if (!mappers.isEmpty() && !reducers.isEmpty()) {
            Set<Integer> keys = mappers.keySet();
            Iterator it = keys.iterator();
            int key;
            while (it.hasNext()) {
                key = (Integer) it.next();
                if (mappers.get(key) != 0) {
                    return false;
                }
            }
            keys = reducers.keySet();
            it = keys.iterator();
            while (it.hasNext()) {
                key = (Integer) it.next();
                if (mappers.get(key) != 0) {
                    return false;
                }
            }
        } else {
            return false;
        }
        return true;
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
