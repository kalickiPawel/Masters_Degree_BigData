package supplement01;

public class MyEdge {
    long src;
    long dst;
    String value;

    public MyEdge(long src, long dst, String value) {
        this.src = src;
        this.dst = dst;
        this.value = value;
    }

    public long getSrc() {
        return src;
    }

    public void setSrc(long src) {
        this.src = src;
    }

    public long getDst() {
        return dst;
    }

    public void setDst(long dst) {
        this.dst = dst;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
