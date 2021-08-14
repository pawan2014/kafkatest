package model;

import lombok.Data;

@Data
public class ShareInput1 {
    String shareId;
    String transcationId;
    String type;
    Double cost;
    Integer units;
}
