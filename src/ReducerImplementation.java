import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @version 1.0
 * implement your Reducer code inside the class
 */
public class  ReducerImplementation implements ReducerImplInterface {

    public ArrayList<Message> result = new ArrayList<>();

    /**
     * the function invoked by the reducer.
     * internally calles the function f
     * @param key keys from mapper
     * @param values values from mapper
     */
    @Override
    public void reduce(String key, ArrayList<String> values) {
        result.add(f(key,values));
    }

    /**
     * function f logic should be written by the user.
     * currently contains logic for word count
     * @param key getting from mapper
     * @param values from mapper
     * @return Message class object that will be used as output
     * the returned object must contain Type Message.MessageType.TYPE_MAPPER_RESULT
     */
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
