package io.kubesure.aggregate.job;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.TimeUtil;

/**
	 * On window end time prospect are aggregated and sorted for sink operator to process  
*/
class AggregateResults extends ProcessWindowFunction<ProspectCompany, AggregatedProspectCompany,
																				Long,TimeWindow>{
        private static final long serialVersionUID = -686876771747690123L;
        private static final Logger log = LoggerFactory.getLogger(AggregateResults.class);

		@Override
		public void process(Long key,
				ProcessWindowFunction<ProspectCompany, 
				AggregatedProspectCompany, Long, TimeWindow>.Context context,
				Iterable<ProspectCompany> elements,
				Collector<AggregatedProspectCompany> out) throws Exception {

				if(log.isInfoEnabled()){
					log.info("Window start time  - {} - {}" , 
											   TimeUtil.ISOString(context.window().getStart()),
											   context.window().hashCode());	
				}	

				AggregatedProspectCompany agpc = new AggregatedProspectCompany();
				for (ProspectCompany pc : elements) {
					agpc.addCompany(pc);
					agpc.setId(pc.getId());
				}

				//remove duplicate elements generated for late events arrived 
				//window.time.seconds and window.time.lateness 
				List<ProspectCompany> distinctProspectCo = agpc.getProspectCompanies()
				 									           .stream()
													           .distinct()
													           .collect(Collectors.toList());
				agpc.setProspectCompanies(distinctProspectCo);									   

				//Sort out of order event by event time 
				agpc.getProspectCompanies().sort(Comparator.comparing(ProspectCompany::getEventTime));

				if(log.isInfoEnabled()){
					for (ProspectCompany pc1 : agpc.getProspectCompanies()) {
						log.info("Agg Event time     - {}" , TimeUtil.ISOString(pc1.getEventTime()));
					}
					log.info("Window end time    - {}" , TimeUtil.ISOString(context.window().getEnd())); 
				}

				out.collect(agpc);		
		}
	}