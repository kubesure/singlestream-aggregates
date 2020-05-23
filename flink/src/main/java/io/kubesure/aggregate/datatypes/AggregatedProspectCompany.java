package io.kubesure.aggregate.datatypes;

import java.util.ArrayList;
import java.util.List;

public class AggregatedProspectCompany {

    private String id;
    private List<ProspectCompany> companies;
    private Long timestamp;

    public AggregatedProspectCompany() {
        this.companies = new ArrayList<ProspectCompany>();
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void addCompany(ProspectCompany company) {
        this.companies.add(company);
    }

    public ProspectCompany getProspectCompany(){
        return companies.get(0);
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Prospect ID- ").append(id).
        append(" Results aggregated - ").append(companies.size());
        return sb.toString();
    }
    
}