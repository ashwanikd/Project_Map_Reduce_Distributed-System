public class Message {

    public static class MessageType {
        public static final int TYPE_SYSTEM = 0;
        public static final int TYPE_COMM = 1;
        public static final int TYPE_MAPPER_RESULT = 2;
        public static final int TYPE_REDUCER_RESULT = 3;
    }

    private String mMessage;

    private String mKey;

    private int mType;

    public String getMessage() {
        return mMessage;
    }

    public String getKey() {
        return mKey;
    }

    public void setMessage(String message) {
        this.mMessage = message;
    }

    public void setKey(String key) {
        this.mKey = key;
    }

    public void setType(int type) {
        this.mType = type;
    }
}
