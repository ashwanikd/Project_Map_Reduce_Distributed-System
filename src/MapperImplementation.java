import java.util.ArrayList;
import java.util.Scanner;

/**
 * @version 1.0
 * implement your Mapper code inside the class
 */
public class MapperImplementation implements MapperImplInterface {

    MessageList mMessageList = new MessageList();

    /**
     * map function invoked by the main Mapper Server
     * @param message message contains the value as the input read from input file
     */
    @Override
    public void map(Message message) {
        MessageList list = f(message.getMessage());
        mMessageList.getMessages().addAll(list.getMessages());
    }

    /**
     * function f needs to be implemented
     * @param arg arg is the list Ls that has been read from the input file
     * @return A MessageList Object containing a list of Message after processing the input
     * Note: the output messages will go randomly to any alive reducer
     */
    @Override
    public MessageList f(String arg) {
        MessageList tempMessageList = new MessageList();
        Scanner scan = new Scanner(arg);
        while (scan.hasNext()) {
            Message m = new Message();
            m.setKey(scan.next());
            m.setMessage("1");
            tempMessageList.add(m);
        }
        return tempMessageList;
    }
}
