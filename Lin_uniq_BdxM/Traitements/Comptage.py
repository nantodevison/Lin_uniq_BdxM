# -*- coding: utf-8 -*-
'''
Created on 28 janv. 2020

@author: martin.schoreisz
Modulede traitements de points de comptage
'''

import re
import geopandas as gp
import pandas as pd
import numpy as np
from difflib import SequenceMatcher
from sklearn.cluster import DBSCAN
from Outils import plus_proche_voisin


def import_bdxm_pct(chemin):
    """
    importer le fichier de comptage ponctuel
    in : 
        chemin : raw string duc hemin du fichier
    """
    cpt_pct=pd.read_excel(chemin)
    cpt_pct.columns=[a.lower() for a in cpt_pct.columns]
    cpt_pct = gp.GeoDataFrame(cpt_pct, geometry=gp.points_from_xy(cpt_pct.longitude, cpt_pct.latitude), crs='EPSG:4326')
    #cpt_pct.crs = {'init' :'epsg:4326'}
    cpt_pct_l93=cpt_pct.to_crs('epsg:2154')
    cpt_pct_l93['x_l93']=cpt_pct_l93.geometry.apply(lambda x : x.x)
    cpt_pct_l93['y_l93']=cpt_pct_l93.geometry.apply(lambda x : x.y)
    return cpt_pct_l93

def mise_en_forme_bdxm_pct(cpt_pct_l93):
    """
    mettre en forme les attributs des donn�es issues de import_bdxm_pct.
    in  : 
        cpt_pct_l93 : df issues de import_bdxm_pct()
    """
    cpt_pct_l93['nom_voie']=cpt_pct_l93.nom_voie.apply(lambda x : re.sub(('é|è|ê'),'e',x.lower().strip()))
    cpt_pct_l93['sens_unique']=cpt_pct_l93.sens_circulation.apply(lambda x : True if SequenceMatcher(None,' '.join(x.split(' ')[:2]).lower(),'sens unique').ratio()>0.8 else False)
    cpt_pct_l93['type_voie']=cpt_pct_l93.nom_voie.apply(lambda x : x.split(' ')[0])
    cpt_pct_l93['suffix_nom_voie']=cpt_pct_l93.nom_voie.apply(lambda x : ' '.join(x.split(' ')[1:]).lower())
    cpt_pct_l93['date_max_cptg']=cpt_pct_l93.observation.apply(lambda x : pd.to_datetime(x.split(' au ')[1],dayfirst=True))
    cpt_pct_l93['sens_circulation']=cpt_pct_l93.sens_circulation.apply(lambda x : re.sub(('é|è|ê'),'e',x.strip().lower()))
    cpt_pct_l93['observation']=cpt_pct_l93.observation.apply(lambda x : x.strip().lower())

def grp_cluster_bdxm_pct(cpt_pct_l93):
    """
    ajout d'un attribut de fourpement des comptages ponctuels par distance
    in : 
       cpt_pct_l93 : df des comptages isssues de  mise_en_forme_bdxm_pct
    """
    data_test_clust=[[x, y] for x, y in zip(cpt_pct_l93.x_l93.tolist(), cpt_pct_l93.y_l93.tolist())]
    db = DBSCAN(eps=200, min_samples=2).fit(data_test_clust)
    labels = db.labels_
    cpt_pct_l93['n_cluster']=labels

def grp_nom_proches_bdxm_pct(cpt_pct_l93):
    """
    affecter un identifiant de groupement des voies ayant un nom proche
    in  : 
        cpt_pct_l93 : df des comptages issues de cluster_bdxm_pct
    """
    cross_join_ncluster=cpt_pct_l93[['ident','nom_voie','type_voie','suffix_nom_voie', 'sens_circulation','annee','n_cluster']].merge(
       cpt_pct_l93[['ident','nom_voie','type_voie','suffix_nom_voie', 'sens_circulation','annee','n_cluster']], on='n_cluster') #avoir toutes les relations internoms possibles
    cross_join_ncluster['comp_nom_voie']=cross_join_ncluster.apply(lambda x : SequenceMatcher(None,x['suffix_nom_voie_x'], x['suffix_nom_voie_y']).ratio(), axis=1)#affecter une note a cahque relation
    voie_nom_proches=cross_join_ncluster.loc[(cross_join_ncluster['comp_nom_voie']>0.85) & (cross_join_ncluster['type_voie_x']==cross_join_ncluster['type_voie_y'])
                            ].sort_values(['n_cluster','ident_x'])#ne conserver qque les relations bien notees
    voie_nom_proches['id_grp_nom_voie']=voie_nom_proches.ident_x.rank(method='dense')#ajouter un id 
    corresp_voies=voie_nom_proches.drop_duplicates('ident_y')
    grp_nom_voie=cpt_pct_l93.merge(corresp_voies[['ident_y','id_grp_nom_voie']].rename(columns={'ident_y':'ident'}), on='ident')#jointure sur id de depart
    return grp_nom_voie

def grp_period_bdxm_pct(grp_nom_voie):
    """
    regrouper les points de comptages ponctuels par periode de comptage equivalente
    in : 
        grp_nom_voie : df issue de grp_nom_proches_bdxm_pct
    """
    grp_period=grp_nom_voie.copy()
    grp_period['date_max_cptg']=grp_period.observation.apply(lambda x : pd.to_datetime(x.split(' au ')[1],dayfirst=True))
    grp_period['id_period']=grp_period.date_max_cptg.rank(method='dense')
    grp_period['indicefinal']=grp_period.n_cluster+(grp_period.id_grp_nom_voie*(1000))+(grp_period.id_period*10000)
    return grp_period

def grp_idtronc_bdxm_pct(cpt_pct_l93,gdf_rhv_groupe,grp_period):
    """
    grouper les comptages ponctuels selon l'idtroncon de la voie le plaus proches.
    necessite d'avoir regrouper le filaire de voie en troncon homogene
    in : 
        cpt_pct_l93 : df des comptages issue de grp_cluster_bdxm_pct
        gdf_rhv_groupe : df du filiare regroupe en troncon homogene
        grp_period : df des comptages issues de grp_period_bdxm_pct
    """
    joint_dist_min=plus_proche_voisin(cpt_pct_l93,gdf_rhv_groupe,20,'ident','ident').merge(gdf_rhv_groupe[['ident','idtronc','numero']],
                        left_on='ident_right',right_on='ident')[['ident_left','idtronc','numero']].rename(columns={'ident_left' : 'ident'})
    grp_troncon=grp_period.merge(joint_dist_min, on='ident',how='left')
    return grp_troncon

def annee_recente_bdxm_pct(grp_troncon):
    """
    ne garder que l'annee la plus recente des comptages
    in : 
        grp_troncon : df issue de grp_idtronc_bdxm_pct
    """
    anne_recente=grp_troncon.loc[grp_troncon.groupby('idtronc')['date_max_cptg'].transform(max)==grp_troncon['date_max_cptg']].copy()
    return anne_recente

def idtroncok_bdxm_pct(anne_recente):
    """
    regrouper les comptages pour les idtronc qui ne supporte que 2 comptages
    """
    idtronc_grp=anne_recente.groupby('idtronc').ident.count()
    idtroncOkTmjo=anne_recente.loc[anne_recente.idtronc.isin(idtronc_grp.loc[idtronc_grp<3].index.tolist())][['ident','idtronc','sens_circulation','tmjo_tv']].copy()
    return idtronc_grp,idtroncOkTmjo
    
def classer_tronc_sup_2pt_bdxm_pct(idtronc_grp,anne_recente):
    """
    trouver les troncons supportantplus de 2 pointsde comptages pct et mettre en forme ces points
    """
    #isoler les idtronc supportant + de 2 pt de comptages
    idtronc_sup2=idtronc_grp.loc[idtronc_grp>2].copy()
    #trouver les points correspondants
    pt_sup2=anne_recente.loc[anne_recente['idtronc'].isin(idtronc_sup2.index.tolist())].copy()
    #ajouter attribut qui traduit le nb de valeurs différentes de sens circulation
    def nb_sens_circu(idtronc) :
        return len(pt_sup2.loc[pt_sup2['idtronc']==idtronc].sens_circulation.unique())  
    pt_sup2['nb_sens_circu']=pt_sup2.apply(lambda x : nb_sens_circu(x['idtronc']), axis=1)
    return pt_sup2

def ptsup2_2senscircu_bdxm_pct(pt_sup2): 
    """
    traiter les points qui sont plus de 2 sur un idtronc homogene, avec seulement 2 sens de crculation
    """
    ptSup2SensCircu2=pt_sup2.loc[pt_sup2['nb_sens_circu']==2].copy()
    ptSup2SensCircu2.drop_duplicates(['nom_voie','sens_circulation','tmjo_tv','observation'],inplace=True)#qq points ont des ident différents mais sont les mêmes
    ptSup2SensCircu2.drop_duplicates(['sens_circulation','tmjo_tv','observation'],inplace=True)#qla mm que la précédente, mais je ne sais pas pourquoi l'ajout de nom_voie fait bugger le drop duplicates pour les ident  716,717,975,976
    ptSup2SensCircu2OkTmjo=ptSup2SensCircu2.groupby(['idtronc','sens_circulation'])['tmjo_tv'].max().reset_index().merge(
        ptSup2SensCircu2[['idtronc','sens_circulation','tmjo_tv','ident']], on=['idtronc','sens_circulation','tmjo_tv'], how='left')
    return ptSup2SensCircu2OkTmjo

def pt_restants_bdxm_pct(pt_sup2):
    """
    traiter tous les cas de troncon avec plus de 2 points et plus de 2 sens de circulation différents
    """
    ptSup2SensCircuSup2=pt_sup2.loc[pt_sup2['nb_sens_circu']>2][['ident','idtronc','sens_circulation','tmjo_tv','indicefinal']].sort_values(['idtronc','indicefinal','sens_circulation']).copy()

    #filtre des points dont le sens circul est le mm ou qui sont isole
    list_ident, list_idtronc, list_indicefinal, list_senscircu=(ptSup2SensCircuSup2.ident.tolist(),ptSup2SensCircuSup2.idtronc.tolist(),
                                                                ptSup2SensCircuSup2.indicefinal.tolist(),ptSup2SensCircuSup2.sens_circulation.tolist())
    list_new_ident=list_ident
    for i in range(len(list_ident)) : 
        if i<len(list_ident)-1 :
            if list_idtronc[i+1]==list_idtronc[i] : 
                if list_indicefinal[i+1]==list_indicefinal[i] :
                    if SequenceMatcher(None,list_senscircu[i+1], list_senscircu[i]).ratio()>0.75 : 
                        list_new_ident[i+1]=list_ident[i]
        else : 
            if list_idtronc[i]==list_idtronc[i-1] : 
                if list_indicefinal[i]==list_indicefinal[i-1] :
                    if SequenceMatcher(None,list_senscircu[i], list_senscircu[i-1]).ratio()>0.75 : 
                        list_new_ident[i]=list_ident[i-1]
    ptSup2SensCircuSup2['ident_final']=np.array(list_ident)
    #filtrer les points isoles
    ptSup2SensCircuSup2_filtre=ptSup2SensCircuSup2.groupby(['idtronc','indicefinal']).nunique()[['ident']].reset_index()
    ptSup2SensCircuSup2=ptSup2SensCircuSup2.loc[ptSup2SensCircuSup2.indicefinal.isin(ptSup2SensCircuSup2_filtre.loc[ptSup2SensCircuSup2_filtre['ident']>1].indicefinal.
                                                                                 tolist())].copy()
    #filtrer les points qui ont des nom des sens circu proches
    ptSup2SensCircuSup2_filtre=ptSup2SensCircuSup2.loc[ptSup2SensCircuSup2['tmjo_tv']==
                                                              ptSup2SensCircuSup2.groupby('ident_final')['tmjo_tv'].transform(max)].copy()
    #filtrer les points qui sont égaux 
    ptSup2SensCircuSup2_filtre=ptSup2SensCircuSup2_filtre.loc[ptSup2SensCircuSup2_filtre['ident']==ptSup2SensCircuSup2_filtre['ident_final']].copy()
    #filtrer les points qui présente toujours plus de 2 identifiant (i.e pb denomination ou pb référentiel ou pb tronc_elementaire)
    grp=ptSup2SensCircuSup2_filtre.groupby('idtronc').nunique()[['ident']].reset_index()
    pt_non_affectes=grp.loc[grp['ident']>2]
    ptSup2SensCircuSup2Oktmjo=ptSup2SensCircuSup2_filtre.loc[ptSup2SensCircuSup2_filtre.idtronc.isin(grp.loc[grp['ident']<3].idtronc.tolist())][['ident','idtronc','sens_circulation','tmjo_tv']].copy()
    return ptSup2SensCircuSup2Oktmjo,pt_non_affectes
    
def affectation_final_bdxm_pct(ptSup2SensCircuSup2Oktmjo,idtroncOkTmjo,ptSup2SensCircu2OkTmjo):  
    """
    regrouepement final des pt de comptages ponctuels, avec corrections manuelles si besoin
    """
    affect_finale=pd.concat([ptSup2SensCircuSup2Oktmjo,idtroncOkTmjo,ptSup2SensCircu2OkTmjo],axis=0, sort=False)
    affect_finale.loc[affect_finale['ident']==606,'idtronc']=affect_finale.loc[affect_finale['ident']==605].idtronc.values[0]
    return affect_finale

def traitements_bdxm_pct(chemin,gdf_rhv_groupe):
    """
    enchainement des traitements pour points de comptages ponctuels
    """
    cpt_pct_l93=import_bdxm_pct(chemin)
    mise_en_forme_bdxm_pct(cpt_pct_l93)
    grp_cluster_bdxm_pct(cpt_pct_l93)
    grp_nom_voie=grp_nom_proches_bdxm_pct(cpt_pct_l93)
    grp_period=grp_period_bdxm_pct(grp_nom_voie)
    grp_troncon=grp_idtronc_bdxm_pct(cpt_pct_l93,gdf_rhv_groupe,grp_period)
    anne_recente=annee_recente_bdxm_pct(grp_troncon)
    idtronc_grp,idtroncOkTmjo=idtroncok_bdxm_pct(anne_recente)
    pt_sup2=classer_tronc_sup_2pt_bdxm_pct(idtronc_grp,anne_recente)
    ptSup2SensCircu2OkTmjo=ptsup2_2senscircu_bdxm_pct(pt_sup2)
    ptSup2SensCircuSup2Oktmjo=pt_restants_bdxm_pct(pt_sup2)[0]
    affect_finale=affectation_final_bdxm_pct(ptSup2SensCircuSup2Oktmjo,idtroncOkTmjo,ptSup2SensCircu2OkTmjo)
    return affect_finale, cpt_pct_l93
    
    
    
    

    