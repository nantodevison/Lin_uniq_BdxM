# -*- coding: utf-8 -*-
'''
Created on 16 déc. 2019

@author: martin.schoreisz

Module de ventilation des trafics issus des points de comptages
'''

import pandas as pd
from Outils import plus_proche_voisin,nb_noeud_unique_troncon_continu
from collections import Counter


def noeud_fv_ligne_ss_trafic(df_trafic, type_carr):
    """
    Trouver les noeuds du fv qui ont des lignes sans trafic qui arrivent
    in : 
        type_carr : string : designe le type de filtre a appliquer :soit on garde que les noeud qui contiennent 1seul NaN ('max 1 NaN')
                            soit tous les noeuds qui contiennent au moins un valeur de trafic ('min 1 ok')
    """
    #1. faire la liste des lignes et des noeuds
    liste_noeuds=pd.concat([df_trafic[['id_ign','source','tmjo_2_sens','cat_rhv','rgraph_dbl']].rename(columns={'source':'noeud'}),
                            df_trafic[['id_ign','target','tmjo_2_sens','cat_rhv','rgraph_dbl']].rename(columns={'target':'noeud'})],axis=0,sort=False)
    liste_noeuds.tmjo_2_sens.fillna(-99,inplace=True)
    #2. Trouver les noeuds qui comporte au moins une valeur connue. On y affecte une valuer True dans un attribut drapeau 'estimable'
    noeud_grp=liste_noeuds.groupby('noeud').agg({'tmjo_2_sens':lambda x : tuple(x),'cat_rhv':lambda x : tuple(x)}).reset_index()
    noeud_grp['nb_nan']=noeud_grp.tmjo_2_sens.apply(lambda x : Counter(x))
    if type_carr == 'min 1 ok' : 
        noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['nb_nan'])>2 and x['nb_nan'][-99]<len(x['nb_nan']) and x['nb_nan'][-99]!=0 else False,axis=1)
    elif type_carr == 'max 1 NaN' :
        noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['nb_nan'])>2 and x['nb_nan'][-99]==1 else False,axis=1)
    return noeud_grp, liste_noeuds


    
class Troncon(object):
    """
    caractériser un troncon continu.
    attribut : 
        id : identifiant 
        df_lgn : dataframe des lignes qui constituent le troncon
        nb_lgn : nb de ligne du troncon,
        noeuds_uniques, noeuds : lists des noeuds de fin de troncon et des noeuds du troncon
        nb_noeuds_uniques, nb_noeuds : nombre de noeuds et de noeud de fin de traoncon
        df_lign_fin_tronc : dataframe des lignes de fin de troncon
    """
    def __init__(self, df_fv, num_tronc):
        """
        constrcuteur
        in : 
            df_fv  : dataframe du filaire de voie avec rond point simplifie et idtroncon
            num_tronc : numero du troncon
        """
        self.id=num_tronc
        self.df_lgn=df_fv.loc[df_fv['idtronc']==num_tronc].copy()
        self.nb_lgn=len(self.df_lgn)
        self.noeuds_uniques, self.noeuds=nb_noeud_unique_troncon_continu(df_fv,num_tronc,'idtronc')
        self.nb_noeuds, self.nb_noeuds_uniques=len(self.noeuds), len(self.noeuds_uniques)
        self.df_lign_fin_tronc=df_fv.loc[((df_fv['source'].isin(self.noeuds_uniques)) | (df_fv['target'].isin(self.noeuds_uniques))) & 
                       (df_fv['idtronc']==num_tronc)]
        self.nb_lgn_tch_noeud_unique=len(self.df_lign_fin_tronc)
    
    def troncon_touche(self,df_fv):
        """
        trouver les troncons qui touchent, avec la valeur du noeud d'intersection
        in : 
            df_fv : dataframe du filaire de voie avec rond point simplifie et idtroncon
        out : 
            df_tronc_tch : dataframe avec idtronc et noeud
        """
        df_tronc_tch=df_fv.loc[((df_fv['source'].isin(self.noeuds_uniques)) | (df_fv['target'].isin(self.noeuds_uniques))) & 
                        (df_fv['idtronc']!=self.id)]
        df_tronc_tch=pd.concat([df_tronc_tch[['idtronc','source']].rename(columns={'source':'noeud'}),
                                df_tronc_tch[['idtronc','target']].rename(columns={'target':'noeud'})], axis=0, sort=False)
        df_tronc_tch=df_tronc_tch.loc[df_tronc_tch['noeud'].isin(self.noeuds_uniques)].copy()
        return df_tronc_tch
    
    def groupe_noeud_route_2_chaussees (self, graph, distance):
        """
        pour un troncon constitue d'une voie a 2 chaussee, obtenirune correspondance entre les 4 noeuds uniques, en les groupant 2 par 2
        in : 
            graph : df des noeuds
            distance : distance max entre les noeuds a grouper
        out : 
            corresp_noeud_uniq : df avec les id a associer
        """
        df_noeud_uniq=graph.loc[(graph['id'].isin(self.noeuds_uniques))]
        corresp_noeud_uniq=plus_proche_voisin(df_noeud_uniq, df_noeud_uniq, distance, 'id', 'id', True)
        return corresp_noeud_uniq
    
    
    
    
    
    
    
    
    