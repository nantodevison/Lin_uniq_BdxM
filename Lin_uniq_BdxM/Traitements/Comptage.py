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
from difflib import get_close_matches,SequenceMatcher
from sklearn.cluster import DBSCAN


def import_bdxm_pct(chemin):
    """
    importer le fichier de comptage ponctuel
    in : 
        chemin : raw string duc hemin du fichier
    """
    cpt_pct=pd.read_excel(chemin)
    cpt_pct.columns=[a.lower() for a in cpt_pct.columns]
    cpt_pct = gp.GeoDataFrame(cpt_pct, geometry=gp.points_from_xy(cpt_pct.longitude, cpt_pct.latitude))
    cpt_pct.crs = {'init' :'epsg:4326'}
    cpt_pct_l93=cpt_pct.to_crs({'init': 'epsg:2154'})
    cpt_pct_l93['x_l93']=cpt_pct_l93.geometry.apply(lambda x : x.x)
    cpt_pct_l93['y_l93']=cpt_pct_l93.geometry.apply(lambda x : x.y)
    return cpt_pct_l93

def mise_en_forme_bdxm_pct(cpt_pct_l93):
    """
    mettre en forme les attributs des données issues de import_bdxm_pct.
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




    