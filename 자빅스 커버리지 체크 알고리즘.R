library(RMySQL)
library(stringr)
library(dplyr)

#DB연결 및 테이블 리스트 출력
conn<-dbConnect(MySQL(), user="ngsr",password="ngsr",dbname="testdb", host="183.96.235.113")
dbListTables(conn)

#message table을 불러와 subs에 저장
messages<-dbReadTable(conn,"message")
subs<-messages[,c("sub","body")]


#status체크를 위한 데이터 프레임 생성
status<-data.frame(kwd=c("ok","problem","unknown","supported","normal"))

#host 체크를 위한 데이터 프레임 생성
host<-data.frame(dbListFields(conn,"alarm"))
colnames(host)<-c("kwd")
host$kwd<-tolower(host$kwd)

#파싱 알고리즘
KeywordParsing <- function(target){
  kwd<-target
  kwd<-gsub("div"," ",target)
  kwd<-gsub("/"," ",kwd)
  kwd<-gsub("[^[:alnum:]///' ]", " ", kwd)
  kwd<-strsplit(kwd,split=" ")
  kwd<-unlist(kwd)
  kwd<-kwd[!nchar(kwd)<=1]
  kwd<-tolower(kwd)
  kwd<-as.data.frame(kwd)
  kwd<-unique(kwd)
}

#호스트 추출 및 확인
host_check<-data.frame()
for(i in 1:length(subs[,1])){
  tg_sub<-KeywordParsing(subs[i,1])
  tg_body<-KeywordParsing(subs[i,2])
  
  if(length(merge(tg_sub,host,by="kwd")[,1])>=1){
    sub_check<-as.character(merge(tg_sub,host,by="kwd")[,1])
  }else{sub_check<-c(0)}
  
  if(length(merge(tg_body,host,by="kwd")[,1])>=1){
    body_check<-as.character(merge(tg_body,host,by="kwd")[,1])
  }else{body_check<-c(0)}
  host_check<-rbind(host_check,cbind(sub_check,body_check))
}
host_check

#Zabbix Status 추출 및 확인
status_check<-data.frame()
for(i in 1:length(subs[,1])){
  pkwd<-KeywordParsing(subs[i,1])
  sck<-merge(pkwd,status,by="kwd")
  status_check<-rbind(status_check,sck)
}
status_check

# **Zabbix_server 체크
zabsvr_check<-data.frame()
for(i in 1:length(subs[,1])){
  tg_sub<-KeywordParsing(subs[i,1])
  tg_body<-KeywordParsing(subs[i,2])
  sub_ck<-as.numeric(grep("zabbix",tg_sub$kwd) && grep("server",tg_sub$kwd))
  body_ck<-as.numeric(grep("zabbix",tg_body$kwd) && grep("server",tg_body$kwd))
  all_ck<-cbind(sub_ck,body_ck)
  zabsvr_check<-rbind(zabsvr_check,all_ck)
}
zabsvr_check
