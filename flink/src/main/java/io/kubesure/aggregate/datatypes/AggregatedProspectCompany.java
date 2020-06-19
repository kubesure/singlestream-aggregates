package io.kubesure.aggregate.datatypes;

import java.util.ArrayList;
import java.util.List;

public class AggregatedProspectCompany {

    private Long id;
    private List<ProspectCompany> companies;

    public AggregatedProspectCompany() {
        this.companies = new ArrayList<ProspectCompany>();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void addCompany(ProspectCompany company) {
        this.companies.add(company);
    }

    public ProspectCompany getProspectCompany(){
        return companies.get(0);
    }

    public List<ProspectCompany> getProspectCompanies(){
        return companies;
    }

    public void setProspectCompanies(List<ProspectCompany> prospectCompanies){
        this.companies = prospectCompanies;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Prospect ID- ").append(id).
        append(" Results aggregated - ").append(companies.size());
        return sb.toString();
    }
}