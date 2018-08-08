#!/usr/bin/env Rscript
#prediction analysis
library(dplyr,quietly = T)
library(data.table, quietly = T)
library(readr, quietly = T)
library(lubridate)
library(sqldf)

options(scipen = 99)
gc()
remove(list_df_bdsa)
setwd('/mnt/hdd1/ritesh/beacon_DSA')
df_bdsa <- readRDS("df_beacon_DSA.rds")

data.frame(table(date(df_bdsa$`__time`), exclude= NULL))

df_bdsa$time <- df_bdsa$`__time`
df_bdsa$`__time` <- NULL

uniqueip <- unique(df_bdsa$ip_address)
nmin <- 10001
nmax <- 13000
gc()
results <- list()

i <- 1
j <- 1

for(ip in uniqueip[nmin:nmax]){
  if(i%%10 == 0){
    print(paste(" done with ..." , as.character(i), sep = " "))
  }
  ip_data <- df_bdsa[which(df_bdsa$ip_address == ip),]
  if(nrow(ip_data) < 5000){
    j <- j + 1
    ip_data_sorted <- sqldf::sqldf("select * from ip_data order by ip_address, time desc")
    hr <- sqldf("select 
                a.ip_address,
                a.time, (a.time - b.time) as seconds_since_last_seen,  
                count(distinct b.device_id) as num_distinct_deviceid_last_1hr,
                count(distinct b.local_ip_address) as num_distinct_local_ip_address_last_1hr,
                count(distinct b.customer_session_id) as num_distinct_customer_session_id_last_1hr,
                count(distinct b.customer_user_id) as num_distinct_customer_user_id_last_1hr,
                count(distinct b.ios_system_version) as num_distinct_ios_device_last_1hr,
                count(distinct b.android_id) as num_distinct_android_device_last_1hr
                from ip_data_sorted a left join ip_data b on a.ip_address = b.ip_address and a.time >= b.time
                where seconds_since_last_seen <= 60*60
                group by a.ip_address, a.time
                order by a.time asc")
    
    hrday <- sqldf("select 
                   a.ip_address,
                   a.time, (a.time - b.time) as seconds_since_last_seen,  
                   count(distinct b.device_id) as num_distinct_deviceid_last_1day,
                   count(distinct b.local_ip_address) as num_distinct_local_ip_address_last_1day,
                   count(distinct b.customer_session_id) as num_distinct_customer_session_id_last_1day,
                   count(distinct b.customer_user_id) as num_distinct_customer_user_id_last_1day,
                   count(distinct b.ios_system_version) as num_distinct_ios_device_last_1day,
                   count(distinct b.android_id) as num_distinct_android_device_last_1day
                   from ip_data_sorted a left join ip_data b on a.ip_address = b.ip_address and a.time >= b.time
                   where seconds_since_last_seen <= 24*3600
                   group by a.ip_address, a.time
                   order by a.time asc")
    
    
    hr7day <- sqldf("select 
                    a.ip_address,
                    a.time, (a.time - b.time) as seconds_since_last_seen,  
                    count(distinct b.device_id) as num_distinct_deviceid_last_7day,
                    count(distinct b.local_ip_address) as num_distinct_local_ip_address_last_7day,
                    count(distinct b.customer_session_id) as num_distinct_customer_session_id_last_7day,
                    count(distinct b.customer_user_id) as num_distinct_customer_user_id_last_7day,
                    count(distinct b.ios_system_version) as num_distinct_ios_device_last_7day,
                    count(distinct b.android_id) as num_distinct_android_device_last_7day
                    from ip_data_sorted a left join ip_data b on a.ip_address = b.ip_address and a.time >= b.time
                    where seconds_since_last_seen <= 7*24*3600
                    group by a.ip_address, a.time
                    order by a.time asc")
    hr30day <- sqldf("select 
                     a.ip_address,
                     a.time, (a.time - b.time) as seconds_since_last_seen,  
                     count(distinct b.device_id) as num_distinct_deviceid_last_30day,
                     count(distinct b.local_ip_address) as num_distinct_local_ip_address_last_30day,
                     count(distinct b.customer_session_id) as num_distinct_customer_session_id_last_30day,
                     count(distinct b.customer_user_id) as num_distinct_customer_user_id_last_30day,
                     count(distinct b.ios_system_version) as num_distinct_ios_device_last_30day,
                     count(distinct b.android_id) as num_distinct_android_device_last_30day
                     from ip_data_sorted a left join ip_data b on a.ip_address = b.ip_address and a.time >= b.time
                     where seconds_since_last_seen <= 30*24*3600
                     group by a.ip_address, a.time
                     order by a.time asc")
    
    ip_all <- sqldf("select * from hr a 
                    left join hrday c on a.ip_address = c.ip_address and a.time = c.time
                    left join hr7day d on a.ip_address = d.ip_address and a.time = d.time
                    left join hr30day e on a.ip_address = e.ip_address and a.time = e.time
                    ")
    
    ip_all$seconds_since_last_seen <- NULL
    results[[i]] <- ip_all
    remove(ip_data_sorted, ip_all)
  }
  remove(ip_data)
  i <- i + 1
  gc()
}


results_Df_p3 <- bind_rows(results)
length(unique(results_Df_p3$ip_address))

saveRDS(results_Df_p3,'results_Df_10kto13K.rds')




results_tot <- rbind(results_Df_3K, results_Df_p2, results_Df_p3)

###process to get rid of time conflict becaose of which I am loosing rows in my aggregations
t_df <- data.frame(table(df_dsa_ip$ip_address))
t_df_agg <- data.frame(table(results_tot$ip_address))
comp <- plyr::join(t_df, t_df_agg, type= "left",by="Var1")
colnames(comp) <- c('ip_address', 'freq_orig', 'freq_agg')

comp$is_agg_mismatch <- ifelse(comp$freq_orig != comp$freq_agg, 1, 0)

##keeping the ips where there is no conflict
ip_clean <- unique(comp[which(comp$is_agg_mismatch == 0), "ip_address"])

#raw dsa for clean ip without conflict
df_dsa_ip <- df_bdsa[which(df_bdsa$ip_address %in% ip_clean),]

#aggregated data for clean ips
results_tot_ip_clean <- results_tot[which(results_tot$ip_address %in% ip_clean),]

#matching dsa with ip agg
df_dsa_ip_w_ip_agg <- sqldf("select * from df_dsa_ip a left join results_tot_ip_clean b on a.ip_address = b.ip_address and a.time = b.time")


## Keeping the records from 7t June onwards to have at least one week of history
df_7thJune_onwards <- df_dsa_ip_w_ip_agg[which(date(df_dsa_ip_w_ip_agg$time)>='2017-06-07'),]


saveRDS(df_7thJune_onwards,'df_7thJune_onwards.rds')