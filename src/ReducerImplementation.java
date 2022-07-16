import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @version 1.0
 * implement your Reducer code inside the class
 */
public class  ReducerImplementation implements ReducerImplInterface {

    public ArrayList<Message> result = new ArrayList<>();

    @Override
    public void reduce(String key, ArrayList<String> values) {
        result.add(f(key,values));
    }

    @Override
    public Message f(String key, ArrayList<String> values) {
        Message message = new Message()
                .setKey(key)
                .setType(Message.MessageType.TYPE_MAPPER_RESULT)
                .setFlag(0);
        Iterator it = values.iterator();
        int res = 0;
        while (it.hasNext()) {
            res++;
            it.next();
        }
        message.setMessage("" + res);
        return message;
    }
}
