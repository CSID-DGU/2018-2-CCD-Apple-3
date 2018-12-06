#Working Directory 지정
#getwd()
#setwd("/Users/jws/Documents/4-2/캡스톤과제/개발/")

#필요 라이브러리
library(RMySQL)
library(stringr)
library(dplyr)
library(data.table)
# library(readxl)

# R Server용 라이브러리
# library(rJava)
# library(Runiversal)
# library(Rserve)

#R Server 오픈
# Rserve(FALSE,port=6311,args='--RS-encoding utf8 --no-save --slave --encoding utf8')
# Rserve(args="--RS- encoding utf8")
dbDisconnect(conn)
#DB연결
conn<-dbConnect(MySQL(), user="ngsr",password="ngsr",dbname="testdb", host="183.96.235.113")

#DB조회
#dbListTables(conn)

#DB에서 정보 가져오기
#usr<-dbReadTable(conn,"users")
unchecked_message<-dbGetQuery(conn,"select * from message where grade = 'unchecked';")
host<-data.frame(tolower(dbListFields(conn,"alarm")))
colnames(host)<-c("kwd")

#status check를 위한 데이터프레임 생성
status<-data.frame(kwd=c("ok","problem","unknown","supported","normal"))

#전처리(파싱) 알고리즘
KeywordParsing <- function(target){
  kwd<-target
  kwd<-gsub("div"," ",target)
  kwd<-gsub("/"," ",kwd)
  kwd<-gsub("[^[:alnum:]///' ]", " ", kwd)
  kwd<-strsplit(kwd,split=" ")
  kwd<-unlist(kwd)
  kwd<-kwd[!nchar(kwd)<=1]
  kwd<-kwd[!nchar(kwd)>50]
  kwd<-tolower(kwd)
  kwd<-as.data.frame(kwd)
  kwd<-unique(kwd)
}

#호스트와 그레이드 분석 후 테이블에 업데이트
for(i in 1:length(unchecked_message[,1])){
  tgm <- unchecked_message[i,]
  if(tgm$sender == "eoth999@gmail.com"){
    set_host_qr<-sprintf("UPDATE message SET host ='%s' WHERE eno='%s' LIMIT 1;","checkserver",tgm$eno)
    set_grade_qr<-sprintf("UPDATE message SET grade ='%s' WHERE eno='%s' LIMIT 1;","fatal",tgm$eno)
    # dbGetQuery(conn, set_host_qr)
    # dbGetQuery(conn, set_grade_qr)
  }else if(tgm$sender == "hmx17c@naver.com"){
    set_host_qr<-sprintf("UPDATE message SET host ='%s' WHERE eno='%s' LIMIT 1;","crash",tgm$eno)
    set_grade_qr<-sprintf("UPDATE message SET grade ='%s' WHERE eno='%s' LIMIT 1;","fatal",tgm$eno)
  }else{
    checked_status<-as.character(merge(KeywordParsing(tgm$sub),status, by = "kwd")[1,1])
    checked_host<-as.character(merge(KeywordParsing(tgm$sub),host, by = "kwd")[1,1])
    if(checked_status == "problem" || checked_status == "unknown"){
      set_grade_qr<-sprintf("UPDATE message SET grade ='%s' WHERE eno='%s' LIMIT 1;","fatal",tgm$eno)
    }else{
      set_grade_qr<-sprintf("UPDATE message SET grade ='%s' WHERE eno='%s' LIMIT 1;","normal",tgm$eno)
    }
    if(is.na(checked_host)){
      if(length(merge(KeywordParsing(tgm$body),host, by = "kwd")[,1])>0){
        set_host_qr<-sprintf("UPDATE message SET host ='%s' WHERE eno='%s' LIMIT 1;",as.character(merge(KeywordParsing(tgm$body),host, by = "kwd")[1,1]),tgm$eno)
      }else if(grep("zabbix",KeywordParsing(tgm$body)$kwd) && grep("server",KeywordParsing(tgm$body)$kwd)){
        set_host_qr<-sprintf("UPDATE message SET host ='%s' WHERE eno='%s' LIMIT 1;","zabbix server",tgm$eno)
      }else{
        set_host_qr<-sprintf("UPDATE message SET host ='%s' WHERE eno='%s' LIMIT 1;","unspecified",tgm$eno)
      }
    }else{
      set_host_qr<-sprintf("UPDATE message SET host ='%s' WHERE eno='%s' LIMIT 1;",checked_host,tgm$eno)
    }
  }
  dbGetQuery(conn,set_host_qr)
  dbGetQuery(conn,set_grade_qr)
}

dbDisconnect(conn)
