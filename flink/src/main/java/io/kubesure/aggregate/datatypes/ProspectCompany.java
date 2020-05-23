package io.kubesure.aggregate.datatypes;

import java.util.ArrayList;
import java.util.List;

public class ProspectCompany {

    private String id;
    private String companyName;
    private String tradeLicenseNumber;
    private boolean match;
    private Long timestamp;
    private List<Prospect> shareHolders;

    public ProspectCompany(String id, String companyName, String tradeLicenseNumber, boolean match) {
        shareHolders = new ArrayList<Prospect>();
        this.id = id;
        this.companyName=companyName;
        this.tradeLicenseNumber=tradeLicenseNumber;
        this.match=match;
    }

    public ProspectCompany(){
        shareHolders = new ArrayList<Prospect>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isMatch() {
        return match;
    }

    public void setMatch(boolean match) {
        this.match = match;
    }

    public String getTradeLicenseNumber() {
        return tradeLicenseNumber;
    }

    public void setTradeLicenseNumber(String tradeLicenseNumber) {
        this.tradeLicenseNumber = tradeLicenseNumber;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public void addShareHolder(Prospect shareHolder) {
        this.shareHolders.add(shareHolder);
    }

    public void setShareHolders(List<Prospect> shareHolders) {
        this.shareHolders = shareHolders;
    }
    
    public List<Prospect> getShareHolders(){
        return shareHolders;
    }
    
}