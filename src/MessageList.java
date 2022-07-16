import java.util.ArrayList;

public class MessageList {
    ArrayList<Message> messages;

    public MessageList() {
        messages = new ArrayList<>();
    }

    public boolean add(Message message) {
        if (message != null) {
            messages.add(message);
            return true;
        } else {
            return false;
        }
    }

    public ArrayList<Message> getMessages() {
        return messages;
    }
}
