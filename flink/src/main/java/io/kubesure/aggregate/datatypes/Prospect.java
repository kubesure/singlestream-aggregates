package io.kubesure.aggregate.datatypes;

@SuppressWarnings("unused")
public class Prospect implements Comparable<Prospect>{

    private static final long serialVersionUID = 1L;

    private Long id;
    private String cif;
    private String account;
    private String firstName;
    private String lastName;
    private String fullName;
    private boolean match;

    public Prospect(Long id, String firstName, String lastName, boolean match) {
        this.id=id;
        this.firstName=firstName;
        this.lastName=lastName;
        this.match=match;
    }

    public void setId(long id){
        this.id = id;
    }

    public long getId() {
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

    public String getCif(){
        return cif;
    }

    public void setCif(String cif){
        this.cif = cif;
    }

    public Prospect(){}

    public int compareTo(Prospect prospect){
           return Long.compare(this.id, prospect.id); 
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
            return ( (this.cif.equals(that.cif)) && (this.id.equals(that.id))); 
        }
        return false;
    }
}