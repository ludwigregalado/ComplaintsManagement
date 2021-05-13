library(tidyverse)
library(lubridate)
library(RODBC)

DWH<-odbcDriverConnect(readLines("connection_R.sql"))
# Ingreso registros de quejas
ruta <- format(floor_date(today()-months(1)), format = "%Y%m")
Inconformidades<-readxl::read_xlsx(paste(ruta,dir(ruta), sep = "/"))
Inconformidades$REPORTE<-as.numeric(substring(Inconformidades$REPORTE,1,7))


Ajustadores <- as.data.frame( sqlQuery(DWH,
"SELECT  
  IdAjustador
, ajustador.Ajustador
, ajustador.Despacho
, reporte.IdLugarAccidente
, convert(char,reporte.NumReporte) as NumReporte
, reporte.TiempoCreacionReporte
, reporte.TiempoInicioAjusteReporte
, reporte.IdOficinaOcurrencia
, reporte.NumReclamo
, ofi.NombreOficina
, ofi.zona
,ofi.NombreEstado

  FROM 
Tb_bi_SinModeloCatalogoAjustador ajustador



INNER JOIN Tb_BI_GrlSinReporte AS reporte ON reporte.NumReporte=ajustador.NoReporte
INNER JOIN  VW_BI_CatOficina ofi on ofi.IdOficina= reporte.IdOficinaOcurrencia
where TiempoInicioAtencionReporte >=getdate() - 700"))

NPS <- as.data.frame(sqlQuery(DWH,
"SELECT *  FROM
Tb_bi_GrlNPS_SurveyMonkey"))

NPS$FechaCreacion = gsub("[a.pm]", "",NPS$FechaCreacion)
NPS$FechaMedición<-substr(NPS$FechaMedición,1,10)

FechaObservacion = floor_date(today(), unit = "month")-months(1)

NPS <- NPS %>%
  filter(year(dmy(FechaMedición))==year(dmy(FechaObservacion)) & month(dmy(FechaMedición))==month(dmy(FechaObservacion))  )

NPSMES = 100*(length(NPS$NPS[NPS$NPS>8])-length(NPS$NPS[NPS$NPS<7]))/length(NPS$NPS)
colnames(NPS)[11] <- "COMENTARIOS"
colnames(NPS)[5] <- "SUBTIPO"
NPS$Queja_Ajuste <- ifelse(NPS$AtencionAjustador<7 |NPS$TiempoLlegadaAjustador<7,1,0)

InconformidadesMes <- Inconformidades[,c("REPORTE", "Canal","Mes", "DEPARTAMENTO","SUBTIPO","COMENTARIOS")]
colnames(InconformidadesMes)[1] <- "NumReporte"
colnames(InconformidadesMes)[3] <- "FechaMedición"
InconformidadesMes$FechaMedición <- as.character(InconformidadesMes$FechaMedición)

#InconformidadesMes$NumReporte<-as.numeric(InconformidadesMes$NumReporte)
NPSMes <- NPS[,c("NumReporte", "Canal","FechaMedición","SUBTIPO","COMENTARIOS","Queja_Ajuste")]



Mes <- InconformidadesMes %>%
  full_join(NPSMes)
Mes$Queja_Ajuste<-ifelse(str_detect(Mes$DEPARTAMENTO,"Ajuste") | str_detect(Mes$SUBTIPO,"Ajustador"),1,Mes$Queja_Ajuste)

Quejas <- Mes


FechaInicio = floor_date(today(), unit = "month")-months(1)
FechaFin = floor_date(today(), unit = "month")
Ajustadores$IdAjustador <- as.character(Ajustadores$IdAjustador)


Ajustadores<-Ajustadores %>%
  group_by(IdAjustador, IdOficinaOcurrencia) %>%
  mutate(SiniestrosAjustador = length(IdAjustador[as.Date(TiempoCreacionReporte) >= as.Date(FechaInicio) & as.Date(FechaFin) > as.Date(TiempoCreacionReporte) ])) %>%
  group_by(Despacho,IdOficinaOcurrencia) %>%
  mutate(SiniestrosDespacho = length(Despacho[as.Date(TiempoCreacionReporte) >= as.Date(FechaInicio) & as.Date(FechaFin) > as.Date(TiempoCreacionReporte) ])) %>%
  group_by(IdOficinaOcurrencia) %>%
  mutate(ReportesOficina=length(IdOficinaOcurrencia[as.Date(TiempoCreacionReporte)>=as.Date(FechaInicio)&as.Date(FechaFin) >as.Date(TiempoCreacionReporte) ]))

QuejasAjustador<-Quejas%>%
  inner_join(Ajustadores, by=c("NumReporte"="NumReporte")) %>%
  filter(Queja_Ajuste==1)

QuejasAjustador <- QuejasAjustador %>%
  group_by(IdAjustador,NombreOficina) %>%
  mutate(QuejasAjustador = n()) %>%
  group_by(Despacho) %>%
  mutate(QuejasDespacho = n())

QuejasAjustador$QuejasPonderadaAjustador <- 100*QuejasAjustador$QuejasAjustador/QuejasAjustador$SiniestrosAjustador
QuejasAjustador$QuejasPonderadaDespacho <- 100*QuejasAjustador$QuejasDespacho/QuejasAjustador$SiniestrosDespacho


Pagos <- as.data.frame(sqlQuery(DWH,
"SELECT 
 oficina.NombreOficina
,Reporte.NumReporte
,val.FechaAutorizacionValuacion
 FROM

Tb_BI_GrlSinValuacion val

INNER JOIN 
        VW_BI_CatOficina AS oficina
      on
        oficina.IdOficina=val.IdOficinaAtencion
        
 inner join Tb_BI_GrlSinReclamo reclamo on reclamo.NumReclamo = val.NumSiniestro

inner join Tb_BI_GrlSinReporte reporte on reclamo.NumReclamo=reporte.NumReclamo
        
 WHERE 
 (val.TipoProceso = 'PAGO DE DAÑOS' OR val.TipoProceso = 'PERDIDA TOTAL' ) AND
 
 val.FechaReporte >=getdate() - 700     

"
))


Valuacion <- as.data.frame( sqlQuery(DWH,"
 SELECT 
 Reporte.NumReporte
,val.MarcaVehiculo
,val.NipTaller
,val.Taller
,val.IdOficinaAtencion
,ofi.NombreOficina
,ofi.zona
,val.FechaReporte
,ofi.NombreEstado

From
Tb_BI_GrlSinValuacion val
inner join VW_BI_CatOficina ofi on ofi.IdOficina= val.IdOficinaAtencion

inner join Tb_BI_GrlSinReclamo reclamo on reclamo.NumReclamo = val.NumSiniestro

inner join Tb_BI_GrlSinReporte reporte on reclamo.NumReclamo=reporte.NumReclamo
  where FechaReporte >=getdate() - 700"))


ValuacionResumido <- as.data.frame(sqlQuery(DWH,paste("

 SELECT 

val.NipTaller
,val.Taller
,val.IdOficinaAtencion
,ofi.NombreOficina
,ofi.zona
,ofi.NombreEstado
, count(*) AS Reparaciones
From
Tb_BI_GrlSinValuacion val
inner join VW_BI_CatOficina ofi on ofi.IdOficina= val.IdOficinaAtencion

inner join Tb_BI_GrlSinReclamo reclamo on reclamo.NumReclamo = val.NumSiniestro

inner join Tb_BI_GrlSinReporte reporte on reclamo.NumReclamo=reporte.NumReclamo
  where FechaReporte >="," '", format(floor_date(today(), unit = "month")-months(1), format = "%Y%m%d"),"'  \n ","AND
  FechaReporte <", " '", format(floor_date(today(), unit = "month"), format = "%Y%m%d"),"'\n","GROUP BY
val.NipTaller
,val.Taller
,val.IdOficinaAtencion
,ofi.NombreOficina
,ofi.zona
,ofi.NombreEstado", sep = "")))



QuejasTaller <- Quejas %>%
  inner_join(Valuacion, by=c("NumReporte"="NumReporte")) %>%
  filter(DEPARTAMENTO =="Reparación")


Valuacion$MesInteres <- ifelse(as.Date(Valuacion$FechaReporte) >= as.Date(FechaInicio) & as.Date(FechaFin) > as.Date(Valuacion$FechaReporte),1,0)

Valuacion <- Valuacion %>%
  group_by(Taller,NombreOficina) %>%
  mutate(Reparaciones=sum(MesInteres)) %>%
  group_by(NombreOficina) %>%
  mutate( AtencionesOficina=sum(MesInteres))

QuejasTaller <- Quejas %>%
  inner_join(Valuacion, by = c("NumReporte"="NumReporte")) %>%
  filter(DEPARTAMENTO =="Reparación" ) %>%
  group_by(Taller,NombreOficina,AtencionesOficina) %>%
  mutate(QuejasTaller=n()) %>%
  group_by(NombreOficina) %>%
  mutate(QuejasOficina = n())

QuejasTaller$QuejasTallerPonderada <- 100*QuejasTaller$QuejasTaller/QuejasTaller$Reparaciones
QuejasTaller$QuejasOficinaPonderada <- 100*QuejasTaller$QuejasOficina/QuejasTaller$AtencionesOficina


PagosQuejas <- Quejas %>%
  filter(DEPARTAMENTO =="Pagos" | str_detect(SUBTIPO,"pago")) %>%
  inner_join(Pagos) %>%
  group_by(NombreOficina) %>%
  summarise(Quejas = n())

PagosResumido <- Pagos %>%
  group_by(NombreOficina) %>%
  summarise(Pagos=length(NombreOficina[as.Date(FechaAutorizacionValuacion)>=as.Date(FechaInicio)&as.Date(FechaFin) >as.Date(FechaAutorizacionValuacion) ]))

PagosQuejas <- PagosQuejas %>%
  inner_join(PagosResumido)

PagosQuejas$QuejaPonderada <- 100*PagosQuejas$Quejas/PagosQuejas$Pagos

TalleresQuejas <- distinct(QuejasTaller[,c("NombreOficina","Taller","QuejasTaller","Reparaciones","QuejasTallerPonderada")])%>%
  filter(Reparaciones > 0) %>%
  distinct()

Q1 = quantile(TalleresQuejas$Reparaciones, 0.25)
Q2 = quantile(TalleresQuejas$Reparaciones, 0.5)
Q3 = quantile(TalleresQuejas$Reparaciones, 0.75)
DIQ = Q3-Q1
OE = as.numeric(Q2+3*DIQ)

TalleresQuejas$Grupo = ifelse(TalleresQuejas$Reparaciones >= OE, 1, 2)



AjustadoresQuejas <- distinct(QuejasAjustador[,c("NombreOficina","Ajustador","QuejasAjustador","SiniestrosAjustador","QuejasPonderadaAjustador")])%>%
  filter(SiniestrosAjustador > 0) %>%
  distinct()

# Q1=quantile(AjustadoresQuejas$SiniestrosAjustador,.25)
# Q2=quantile(AjustadoresQuejas$SiniestrosAjustador,.5)
# Q3=quantile(AjustadoresQuejas$SiniestrosAjustador,.75)
# DIQ=Q3-Q1
# OE=as.numeric(Q2+3*DIQ)
# 
# TalleresQuejas$Grupo=ifelse(AjustadoresQuejas$SiniestrosAjustador>=OE,1,2)

DespachoQuejas <- distinct(QuejasAjustador[,c("NombreOficina","Despacho","QuejasDespacho","SiniestrosDespacho","QuejasPonderadaDespacho")])%>%
  filter(SiniestrosDespacho > 0) %>%
  distinct()

OficinaQuejasPago <- PagosQuejas %>%
  filter(Pagos>0) %>%
  distinct()

OficinaQuejas <- distinct(QuejasTaller[,c("NombreOficina","AtencionesOficina","QuejasOficina","QuejasOficinaPonderada")])%>%
  filter(AtencionesOficina > 0) %>%
  distinct()

Q1 = quantile(OficinaQuejas$AtencionesOficina, 0.25)
Q2 = quantile(OficinaQuejas$AtencionesOficina, 0.5)
Q3 = quantile(OficinaQuejas$AtencionesOficina, 0.75)
DIQ = Q3-Q1
OE = as.numeric(Q2+3*DIQ)

OficinaQuejas$Grupo = ifelse(OficinaQuejas$AtencionesOficina >= OE, 1, 2)

OficinaQuejas <- OficinaQuejas %>%
  group_by(Grupo) %>%
  mutate(ParticipacionGrupo=100*AtencionesOficina/sum(AtencionesOficina)) %>%
  filter(Grupo == 1 | ParticipacionGrupo >= 2)

median(TalleresQuejas$QuejasTallerPonderada)

PagosQuejascomentarios <- Quejas %>%
  filter(DEPARTAMENTO =="Pagos"| str_detect(SUBTIPO,"pago")) %>%
  left_join(Pagos) %>%
  distinct()

write.csv(TalleresQuejas,paste(ruta,"TalleresQuejas.csv", sep = "/"),col.names = FALSE)
write.csv(AjustadoresQuejas,paste(ruta,"AjustadoresQuejas.csv", sep = "/"),col.names = FALSE)
write.csv(DespachoQuejas,paste(ruta,"DespachoQuejas.csv", sep = "/"),col.names = FALSE)
write.csv(OficinaQuejasPago,paste(ruta,"OficinaQuejasPago.csv", sep = "/"),col.names = FALSE)
write.csv(OficinaQuejas,paste(ruta,"OficinaQuejasReparacion.csv", sep = "/"),col.names = FALSE)
write.csv(QuejasAjustador,paste(ruta,"QuejasAjuste.csv", sep = "/"),col.names = FALSE)
write.csv(PagosQuejascomentarios,paste(ruta,"QuejasPago.csv", sep = "/"), col.names=FALSE)
write.csv(QuejasTaller,paste(ruta,"QuejasReparacion.csv", sep = "/"), col.names=FALSE)
