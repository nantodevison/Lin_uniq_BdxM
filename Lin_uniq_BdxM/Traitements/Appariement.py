# -*- coding: utf-8 -*-
'''
Created on 20 d�c. 2019

@author: martin.schoreisz
Module d'apariement entre FV, MMM, BdTopo
'''

import pandas as pd
from Outils import plus_proche_voisin

def corresp_noeud_mmm(df, num_noeud) : 
    """
    creer une df de correspondance entre les differentes lignes arrivant sur un noeud du MMM.
    en entree : 
        df : df du mmm simplifie (contient au moins les trafic (tmja_tv) et les noeud de debut et de fin (FROMNODENO & TONODENO)
             ainsi que l'id (NO))
    """
    #trouver les lignes correspondantes
    lgn_noeud=df.loc[(df['FROMNODENO']==num_noeud) | (df['TONODENO']==num_noeud) ].copy()
    #creer tableaux de correspondance interne MMM
    lgn_noeud['noeud']=num_noeud
    cross_join=lgn_noeud[['noeud','NO','tmja_tv']].merge(lgn_noeud[['noeud','NO','tmja_tv']], on='noeud')
    cross_join['filtre']=cross_join.apply(lambda x : tuple(sorted((x['NO_x'],x['NO_y']))),axis=1)
    cross_join.drop_duplicates('filtre', inplace=True)
    tab_corresp_mmm=cross_join.loc[cross_join['NO_x']!=cross_join['NO_y']].drop('filtre',axis=1).copy()
    return tab_corresp_mmm

def corresp_noeud_rhv(df, num_noeud) :
    """
    la m�me qu'au dessus pour le rhv
    """
    lgn_noeud=df.reset_index().loc[(df.reset_index()['source']==num_noeud) | (df.reset_index()['target']==num_noeud)]
    lgn_noeud['noeud']=num_noeud
    cross_join=lgn_noeud[['noeud','ident','tmjo_2_sens','cat_rhv']].merge(lgn_noeud[['noeud','ident','tmjo_2_sens','cat_rhv']], on='noeud')
    cross_join['filtre']=cross_join.apply(lambda x : tuple(sorted((x['ident_x'],x['ident_y']))),axis=1)
    cross_join.drop_duplicates('filtre', inplace=True)
    tab_corresp_rhv=cross_join.loc[cross_join['ident_x']!=cross_join['ident_y']].drop('filtre',axis=1).copy()
    return tab_corresp_rhv

def appariement_noeud_mmm_fv(noeuds_mmm, noeud_fv, distance):
    """
    trouver les correspondances entre les noeuds du mmm et du fv
    """
    appariement=pd.concat([plus_proche_voisin(noeuds_mmm,noeud_fv,distance,'id','id').rename(columns={'id_left':'id_mmm', 'id_right':'id_fv'}),
                 plus_proche_voisin(noeud_fv,noeuds_mmm,distance,'id','id').rename(columns={'id_left':'id_fv', 'id_right':'id_mmm'})],
                axis=0, sort=False).drop_duplicates()
    return appariement