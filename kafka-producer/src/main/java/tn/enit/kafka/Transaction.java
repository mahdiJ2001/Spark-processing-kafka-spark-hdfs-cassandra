package tn.enit.kafka;

import java.io.Serializable;
import java.util.Date;
import com.fasterxml.jackson.annotation.JsonFormat;

public class Transaction implements Serializable {
    private String id;
    private String ownerId;
    private String receiverId;
    private double amount;
    private String category;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "MST")
    private Date time; // Changed from String to Date for consistency with timestamp formatting

    private String status;

    // Location attribute
    private String location;

    // Default constructor
    public Transaction() {
    }

    // All-args constructor
    public Transaction(String id, String ownerId, String receiverId, double amount, String category, Date time, String status, String location) {
        this.id = id;
        this.ownerId = ownerId;
        this.receiverId = receiverId;
        this.amount = amount;
        this.category = category;
        this.time = time;
        this.status = status;
        this.location = location;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}