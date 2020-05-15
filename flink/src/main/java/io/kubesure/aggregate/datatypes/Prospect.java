package io.kubesure.aggregate.datatypes;

@SuppressWarnings("unused")
public class Prospect implements Comparable<Prospect>{

    private static final long serialVersionUID = 1L;

    private String id;
    private String cif;
    private String account;
    private String firstName;
    private String lastName;
    private String fullName;
    private boolean match;
    private Long timestamp;

    public Prospect(String id, String firstName, String lastName, boolean match) {
        this.id=id;
        this.firstName=firstName;
        this.lastName=lastName;
        this.match=match;
    }

    public void setId(String id){
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setFullName(String fullName){
        this.fullName = fullName;
    }

    public String getFullName() {
        return fullName;
    }

    public void setLastName(String lastName){
        this.lastName = lastName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setFirstName(String firstName){
        this.firstName = firstName;
    }

    public String getFirstName() {
        return firstName;
    }

    public boolean isMatch() {
        return match;
    }

    public void setMatch(boolean match) {
        this.match = match;
    }

    public void setAccount(String account){
        this.account = account;
    }

    public String getAccount() {
        return account;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getCif(){
        return cif;
    }

    public void setCif(String cif){
        this.cif = cif;
    }

    public Prospect(String cif,String account, Long timestamp){
        this.cif = cif;
        this.account = account;
        this.timestamp = timestamp;
    }

    public Prospect(String cif,String firstName, String lastName,Long timestamp){
        this.cif = cif;
        this.timestamp = timestamp;
    }
    
    public Prospect(){}

    public int compareTo(Prospect customer){
           return Long.compare(this.timestamp, customer.timestamp); 
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Customer CIF- ").append(cif).
        append(" Id - ").append(id).
        append(" First Name - ").append(firstName).
        append(" Last Name - ").append(lastName).
        append(" isMatch - ").append(match);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            Prospect that = (Prospect) o;
            return ( (this.cif.equals(that.cif)) && (this.timestamp.equals(that.timestamp)) ); 
        }
        return false;
    }
}