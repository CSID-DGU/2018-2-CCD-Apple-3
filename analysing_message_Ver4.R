#필요 라이브러리
library(RMySQL)
library(stringr)
library(dplyr)
library(data.table)
library(httr)
# library(jsonlite)
# library(readxl)

# R Server용 라이브러리
# library(rJava)
# library(Runiversal)
# library(Rserve)

#R Server 오픈
# Rserve(FALSE,port=6311,args='--RS-encoding utf8 --no-save --slave --encoding utf8')
# Rserve(args="--RS- encoding utf8")
#dbDisconnect(conn)
#DB연결
#conn<-dbConnect(MySQL(), user="ngsr",password="ngsr",dbname="testdb", host="183.96.235.151")
conn<-dbConnect(MySQL(), user="example",password="example123",dbname="example", host="braverokmc79.cafe24.com")
#DB조회
#dbListTables(conn)

#DB에서 정보 가져오기
#usr<-dbReadTable(conn,"users")
unchecked_message<-dbGetQuery(conn,"select * from MESSAGE where grade = 'unchecked' AND eno IS NOT NULL;")
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
if(length(unchecked_message[,1]) == 0 ){
  dbDisconnect(conn)
}else{
  for(i in 1:length(unchecked_message[,1])){
    tgm <- unchecked_message[i,]
    if(tgm$sender == "check@info.kr"){
      set_host <-c("checkserver")
      set_grade<-c("fatal")
      set_host_qr<-sprintf("UPDATE MESSAGE SET host ='%s' WHERE eno='%s' LIMIT 1;",set_host,tgm$eno)
      set_grade_qr<-sprintf("UPDATE MESSAGE SET grade ='%s' WHERE eno='%s' LIMIT 1;",set_grade,tgm$eno)
      # dbGetQuery(conn, set_host_qr)
      # dbGetQuery(conn, set_grade_qr)
    }else if(tgm$sender == "crash@info.kr"){
      set_host <-c("crash")
      set_grade<-c("fatal")
      set_host_qr<-sprintf("UPDATE MESSAGE SET host ='%s' WHERE eno='%s' LIMIT 1;","crash",tgm$eno)
      set_grade_qr<-sprintf("UPDATE MESSAGE SET grade ='%s' WHERE eno='%s' LIMIT 1;","fatal",tgm$eno)
    }else{
      checked_status<-as.character(merge(KeywordParsing(tgm$sub),status, by = "kwd")[1,1])
      checked_host<-as.character(merge(KeywordParsing(tgm$sub),host, by = "kwd")[1,1])
      if(checked_status == "problem" || checked_status == "unknown"){
        set_grade<-c("fatal")
        set_grade_qr<-sprintf("UPDATE MESSAGE SET grade ='%s' WHERE eno='%s' LIMIT 1;",set_grade,tgm$eno)
      }else{
        set_grade<-c("normal")
        set_grade_qr<-sprintf("UPDATE MESSAGE SET grade ='%s' WHERE eno='%s' LIMIT 1;","normal",tgm$eno)
      }
      if(is.na(checked_host)){
        if(length(merge(KeywordParsing(tgm$body),host, by = "kwd")[,1])>0){
          set_hot<-as.character(merge(KeywordParsing(tgm$body),host, by = "kwd")[1,1])
          set_host_qr<-sprintf("UPDATE MESSAGE SET host ='%s' WHERE eno='%s' LIMIT 1;",set_host,tgm$eno)
        }else if(length(grep("zabbix",KeywordParsing(tgm$body)$kwd)) && length(grep("server",KeywordParsing(tgm$body)$kwd))){
          set_host <-c("ZabbixServer")
          set_host_qr<-sprintf("UPDATE MESSAGE SET host ='%s' WHERE eno='%s' LIMIT 1;",set_host,tgm$eno)
        }else{
          set_host <-c("unspecified")
          set_host_qr<-sprintf("UPDATE MESSAGE SET host ='%s' WHERE eno='%s' LIMIT 1;",set_host,tgm$eno)
        }
      }else{
        set_host<-checked_host
        set_host_qr<-sprintf("UPDATE MESSAGE SET host ='%s' WHERE eno='%s' LIMIT 1;",set_host,tgm$eno)
      }
    }
    print(i)
    print(set_host_qr)
    print(set_grade_qr)
    dbGetQuery(conn,set_host_qr)
    dbGetQuery(conn,set_grade_qr)
    if(set_grade == "fatal"){
      users<-dbGetQuery(conn,"select * from USER;")
      alarm_check<-dbGetQuery(conn,"select * from alarm;")
      colnames(alarm_check)<-tolower(colnames(alarm_check))
      for(user in 1:length(users[,1])){
        alarm_user<-alarm_check[alarm_check$user_id==users[user,]$user_id,]
        if(as.integer(!is.na(alarm_user[1,c(set_host)])) >0){
          jandi_url<-alarm_user$jandi
          host_title<-sprintf("Host : %s",set_host)
          POST(url=jandi_url, body = list(body = "Fatal Error가 발생했습니다", connectColor="#E82C0C", connectInfo = data.frame(title =as.character(host_title),description=tgm$sub)), encode = "json",
               add_headers(.headers = c("Accept"= "application/vnd.tosslab.jandi-v2+json","Content-Type"="application/json" )))
        }

      }
    }
  }
}
dbDisconnect(conn)
