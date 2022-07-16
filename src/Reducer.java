import org.w3c.dom.Text;

import java.net.*;
import java.io.*;
public class Reducer implements  ReducerImplementation<Text,IntWritable,NullWrittable,Text>{
    private Socket socket            = null;
    private DataInputStream  input   = null;
    private DataOutputStream out     = null;
    private static final int mMapperStartingPort = 2001;

    private static final int mReducerStartingPort = 3001;

    private static final int mMainPort = 4001;


    public Reducer(String address, int port)
     {
         try
        {
        socket = new Socket (address, port);
        System.out.println("Connected");
        input = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        }
        catch(UnknownHostException u)
        {
            System.out.println(u);
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
         String line = "";
         while(true)
         {
         try
         {
             line = input .readline();
             out.writeUTF(line);

         }
         catch(IOException i)
         {
             System.out.println(i);
         }
         }
     try
     {
        input.close();
        out.close();
        socket.close();
     }
     catch(IOException i)
     {
         System.out.println(i);
     }


     }
    public static void main(String args[]) {
        System.out.println("this is reducer " + args[0]);
        Client client2 = new Client("127.0.0.1", mMapperStartingPort);
        Client client = new Client("127.0.0.1", mReducerStartingPort);
        }
    }
