package com.juejin.im.session;

public class Session {

    /**
     * 用户的唯一性标识
     */
    private String userId;

    private String userName;

    public Session() {
    }

    public Session(String userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return userId + "：" + userName;
    }
}
