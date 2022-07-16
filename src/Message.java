public class Message {

    public static class MessageType {
        public static final int TYPE_SYSTEM = 0;
        public static final int TYPE_COMM = 1;
        public static final int TYPE_MAPPER_RESULT = 2;
        public static final int TYPE_REDUCER_RESULT = 3;
        public static final int TYPE_MAPPER_CHECK = 4;
    }

    public static class MessageFlag {
        public static final int TYPE_ABSENT = 0;
        public static final int TYPE_PRESENT = 1;
        public static final int END = 2;
    }

    private String mMessage;

    private String mKey;

    private int mType;

    private int mFlag;

    public String getMessage() {
        return mMessage;
    }

    public String getKey() {
        return mKey;
    }

    public int getType() {
        return mType;
    }

    public int getFlag() {
        return mFlag;
    }

    public Message setMessage(String message) {
        this.mMessage = message;
        return this;
    }

    public Message setKey(String key) {
        this.mKey = key;
        return this;
    }

    public Message setType(int type) {
        this.mType = type;
        return this;
    }

    public Message setFlag(int flag) {
        this.mFlag = flag;
        return this;
    }
}
