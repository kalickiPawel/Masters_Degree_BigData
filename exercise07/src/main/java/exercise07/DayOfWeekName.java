package exercise07;

public class DayOfWeekName {
    Integer order_dow;
    String day_name;

    public DayOfWeekName(Integer order_dow, String day_name) {
        this.order_dow = order_dow;
        this.day_name = day_name;
    }

    public Integer getOrder_dow() {
        return order_dow;
    }

    public void setOrder_dow(Integer order_dow) {
        this.order_dow = order_dow;
    }

    public String getDay_name() {
        return day_name;
    }

    public void setDay_name(String day_name) {
        this.day_name = day_name;
    }
}
