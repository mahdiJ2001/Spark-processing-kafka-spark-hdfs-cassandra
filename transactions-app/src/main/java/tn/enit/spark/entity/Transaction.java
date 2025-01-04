package tn.enit.spark.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L; // Recommended for Serializable classes

    private String id;
    private String ownerId;
    private String receiverId;
    private double amount;
    private String category;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "MST")
    private Date time; // Date type for timestamp consistency

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

    // Override toString for easier debugging
    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + id + '\'' +
                ", ownerId='" + ownerId + '\'' +
                ", receiverId='" + receiverId + '\'' +
                ", amount=" + amount +
                ", category='" + category + '\'' +
                ", time=" + time +
                ", status='" + status + '\'' +
                ", location='" + location + '\'' +
                '}';
    }

    // Override equals and hashCode for correct comparison and usage in collections
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;
        Transaction that = (Transaction) o;
        return Double.compare(that.amount, amount) == 0 &&
                id.equals(that.id) &&
                ownerId.equals(that.ownerId) &&
                receiverId.equals(that.receiverId) &&
                category.equals(that.category) &&
                time.equals(that.time) &&
                status.equals(that.status) &&
                location.equals(that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ownerId, receiverId, amount, category, time, status, location);
    }
}