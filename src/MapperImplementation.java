import java.util.ArrayList;
import java.util.Scanner;

/**
 * @version 1.0
 * implement your Mapper code inside the class
 */
public class MapperImplementation implements MapperImplInterface {

    MessageList mMessageList = new MessageList();

    @Override
    public void map(Message message) {
        MessageList list = f(message.getMessage());
        mMessageList.getMessages().addAll(list.getMessages());
    }

    /**
     * function needs to be implemented
     * @param arg
     * @return
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
