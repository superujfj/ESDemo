package com.supershan.es;

public class UserBean {
    private int id;
    private String name;
    private String subject;
    private long score;
    private String password;

    public UserBean(int id, String name, String subject, long score, String password) {
        this.id = id;
        this.name = name;
        this.subject = subject;
        this.score = score;
        this.password = password;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public long getScore() {
        return score;
    }

    public void setScore(long score) {
        this.score = score;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
