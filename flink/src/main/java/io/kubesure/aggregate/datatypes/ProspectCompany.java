package io.kubesure.aggregate.datatypes;

import java.util.ArrayList;
import java.util.List;

public class ProspectCompany {

    private String id;
    private List<Prospect> shareHolders;
    private String companyName;
    private String tradeLicenseNumber;
    private boolean match;
    private Long timestamp;

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

    public String getTradeId() {
        return tradeLicenseNumber;
    }

    public void setTradeId(String tradeId) {
        this.tradeLicenseNumber = tradeId;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public void addShareHolder(Prospect shareHolder) {
        shareHolders.add(shareHolder);
    }
    
    public List<Prospect> getShareHolders(){
        return shareHolders;
    }
    
}