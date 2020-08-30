import datetime
import os

import time
import requests
from bs4 import BeautifulSoup, SoupStrainer

import pandas as pd
from dateutil.parser import *

headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36', 'te-cal-countries':'isr'}
my_cookies = {'te-cal-countries': 'afg,alb,dza,asm,and,ago,aia,atg,arg,arm,abw,aus,aut,aze,bhs,bhr,bgd,brb,blr,bel,blz,ben,bmu,btn,bol,bih,bwa,bra,brn,bgr,bfa,bdi,khm,cmr,can,cpv,cym,caf,tcd,chi,chl,chn,cxr,col,ccc,com,cod,cok,cri,null,hrv,cub,cyp,cze,dnk,dji,dma,dom,eap,tls,ecu,egy,slv,gnq,eri,est,eth,emu,eca,eun,flk,fro,fji,fin,null,fra,pyf,gab,gmb,geo,deu,gha,gib,grc,grl,grd,gum,gtm,gin,gnb,guy,hti,hpc,hic,noc,oec,hnd,hkg,hun,isl,ind,idn,irn,irq,irl,imy,isr,ita,civ,jam,jpn,jor,kaz,ken,kir,unk,kwt,kgz,lao,lac,lva,ldc,lbn,lso,lbr,lby,lie,ltu,lmy,lic,lmc,lux,mac,mkd,mdg,mwi,mys,mdv,mli,mlt,mhl,mrt,mus,myt,mex,fsm,mna,mic,mda,mco,mng,mne,msr,mar,moz,mmr,nam,npl,nld,ant,ncl,nzl,nic,ner,nga,nfk,prk,mnp,nor,omn,oth,pak,plw,pse,pan,png,pry,per,phl,pcn,pol,prt,pri,qat,null,cog,reu,rou,rus,rwa,wsm,smr,stp,sau,sen,srb,syc,sle,sgp,svk,svn,slb,som,zaf,sas,kor,ssd,esp,lka,shn,kna,lca,spm,vct,ssa,sdn,sur,swz,swe,che,syr,twn,tjk,tza,tha,tgo,tkl,ton,tto,tun,tur,tkm,tuv,uga,ukr,are,gbr,usa,umc,ury,uzb,vut,ven,vnm,vir,wlf,wbg,wld,yem,zmb,zwe'}

url = "https://tradingeconomics.com/calendar"
page = requests.get(url, headers=headers, cookies=my_cookies)
trs = BeautifulSoup(page.text, 'html.parser', parse_only =  SoupStrainer("tr")) #parse all records
panda_rows = []

#the first <tr> is irrelevnt, so start from the second <tr>
#each table has 2 headers, so we take the first and skip the other by doing double jump each iteration
for record in trs.contents[1: :2]: 
    style =  record.get('style') if record != None else 'break' #distinguish bewtween different tables using there <thead style>
    if style == 'break':
        break
    if style == 'white-space: nowrap': #this tag holds the table's date
        date = record.text.split('Actual')[0].strip()
        record=record.find_next_sibling()
        nextStyle = record.get('style')
        if(nextStyle) == 'white-space: nowrap':  #if the next <tr> is a table header - skip it we dont need it
            record=record.find_next_sibling()
        style = record.get('style')
        while style != 'white-space: nowrap': #iterate all records untill the next <thead> that belongs to the next date's table
                if record.get('data-id') != None: #scrape only revelent <tr> - which are event records distinguisable by their 'data-id' tag
                    for importance in range(1, 4):
                        time_cell = record.select(f".calendar-date-{importance}")
                        if len(time_cell) > 0:
                            break
                    time = time_cell[0].text.strip() if len(time_cell) > 0 else ''  # incase time is missing
                    data_id = record['data-id']
                    country = record['data-country'].title()
                    actual = record.find(id='actual').text
                    prev = record.find(id='previous').text
                    time_stamp = parse(date+ ' ' + time)  # clean and parse time text to UTC datetime object
                    event = (lambda x: x.text if type(x) != type(None) else record['data-event'])(record.find(
                        class_="calendar-event"))  # sometimes calender_event <td> is missing, then take the event from main <tr> tag
                    try:  # if a record has a missing symbol, exception has to be handled
                        symbol = record['data-symbol']
                    except:
                        symbol = ''
                    row = {"Importance": importance, "Actual": actual, "CalendarId": data_id, "Country": country, "Event": event,
                           "Date": time_stamp, "Symbol": symbol, "Previous": prev, }
                    panda_rows.append(row)  # add current event to list that will be the pandas table
                record=record.find_next_sibling()
                if record == None:
                    break
                style = record.get('style')
                
df = pd.DataFrame(
    panda_rows)  # parse the list's table into a pandas DataFrame - that's faster than adding each records to a pabda DataFrame recording to StackOverFlow
df.to_csv('table.csv')
