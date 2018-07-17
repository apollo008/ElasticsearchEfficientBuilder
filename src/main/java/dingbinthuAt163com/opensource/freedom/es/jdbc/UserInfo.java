package dingbinthuAt163com.opensource.freedom.es.jdbc;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 10:29
 */
import java.io.Serializable;

public class UserInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private int id;
    private String username;
    private String password;

    public UserInfo() {
        // TODO Auto-generated constructor stub
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "UserInfo [id=" + id + ", username=" + username + ", password=" + password + "]";
    }
}


