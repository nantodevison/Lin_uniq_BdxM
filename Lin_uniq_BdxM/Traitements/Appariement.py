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

def estim_mmm_jointure_voies(matrice_voie_rhv,cle_mmm_rhv,lgn_rdpt):
    """
    depuis les voies rhv d'un noeud, trouver les voies mmm correspondantes
    in : 
        matrice_voie_rhv : issu de corresp_noeud_rhv
        cle_mmm_rhv : issu du travail postgis
        lgn_rdpt : df des lignes appartenenat a un rond point. issu de la dmearche interne de simplification des troncon
    out : 
        cle_mmm_rhv
    """
    joint_fv_mmm_e1=matrice_voie_rhv.merge(cle_mmm_rhv[['NO','ident']], left_on='ident_x', right_on='ident', how='left').drop_duplicates().rename(
        columns={'NO':'NO_x'}).drop('ident', axis=1)
    joint_fv_mmm_e1=joint_fv_mmm_e1.loc[(~joint_fv_mmm_e1['ident_x'].isin(lgn_rdpt.ident.tolist())) & 
                                        (~joint_fv_mmm_e1['ident_y'].isin(lgn_rdpt.ident.tolist()))].copy()
    if joint_fv_mmm_e1.NO_x.isna().any() : 
        raise PasCorrespondanceError(list(set(joint_fv_mmm_e1.loc[joint_fv_mmm_e1.NO_x.isna()].ident_x.tolist())))
    joint_fv_mmm_e2=joint_fv_mmm_e1.merge(cle_mmm_rhv[['NO','ident']], left_on='ident_y', right_on='ident',how='left').drop_duplicates().rename(
            columns={'NO':'NO_y'}).drop('ident', axis=1)
    joint_fv_mmm_e1=joint_fv_mmm_e2.loc[(~joint_fv_mmm_e2['ident_x'].isin(lgn_rdpt.ident.tolist())) & 
                                        (~joint_fv_mmm_e2['ident_y'].isin(lgn_rdpt.ident.tolist()))].copy()
    if joint_fv_mmm_e2.NO_y.isna().any() : 
        raise PasCorrespondanceError(list(set(joint_fv_mmm_e2.loc[joint_fv_mmm_e2.NO_y.isna()].ident_y.tolist())))
    return joint_fv_mmm_e2

def isoler_trafic_inconnu(joint_fv_mmm_e2):
    """
    dans une df issu de estim_mmm_jointure_voies ne garder que les voies avec un des deux trafic non connu,
    et ne conserver que la ligne de categorie la plus proche si plusieurs lignes possibles
    in : 
        joint_fv_mmm_e2 : df des données de lignes netrantes, issue de estim_mmm_jointure_voies
    out : 
        trafic_inconnus_prior_cat : df
    """
    trafic_inconnus=joint_fv_mmm_e2.loc[((joint_fv_mmm_e2.tmjo_2_sens_x.isna()) & (~joint_fv_mmm_e2.tmjo_2_sens_y.isna())) | 
                                     ((~joint_fv_mmm_e2.tmjo_2_sens_x.isna()) & (joint_fv_mmm_e2.tmjo_2_sens_y.isna()))].copy()
    #on ne garde que les ligne de catégorie la plus proche
    trafic_inconnus['diff_cat']=trafic_inconnus.apply(lambda x : abs((int(x['cat_rhv_x'])-int(x['cat_rhv_y']))), axis=1)
    trafic_inconnus_prior_cat=trafic_inconnus.loc[trafic_inconnus['diff_cat']==trafic_inconnus.groupby(['ident_x','ident_y']).diff_cat.transform(min)]
    return trafic_inconnus_prior_cat

def trafic_mmm(trafic_inconnus_prior_cat,mmm_simple):
    """
    associer les trafc MMM aux voies rhv d'un noeud
    in : 
        trafic_inconnus_prior_cat : cf isoler_trafic_inconnu
        mmm_simple : df des voies mmm avec une valeur de tmja
    """
    traf_mmm=trafic_inconnus_prior_cat.merge(mmm_simple[['NO','tmja_tv']], left_on='NO_x', right_on='NO').drop('NO', axis=1).merge(
    mmm_simple[['NO','tmja_tv']], left_on='NO_y', right_on='NO').drop('NO', axis=1)
    traf_mmm['traf_max']=traf_mmm.apply(lambda x:max(x['tmja_tv_x'],x['tmja_tv_y']), axis=1)
    return traf_mmm

def calcul_trafic_rhv_depuisMMM(trafc_rens, df_tot):
    """
    sur un noeud, calculer les trafic RHV à partir des trafics MMM et d'au moins un trafic RHV connu
    in : 
        traf_mmm : cf trafic_mmm
        df_tot : df de ase des lignes a renseigner. fourni l'idtronc
    out :
        trafc_fin : df des lignes du noeud avec la valeur de trafic et l'idtronc correspodant     
    """
    trafc_rens['ident_a_rens']=trafc_rens.apply(lambda x : x['ident_x'] if pd.isnull(x['tmjo_2_sens_x'])
                                                  else x['ident_y'], axis=1)
    trafc_rens['tmjo_2_sens_extrapol']=trafc_rens.apply(lambda x : 
        int(x['tmja_tv_y']/x['tmja_tv_x']*x['tmjo_2_sens_x']) if pd.isnull(x['tmjo_2_sens_y']) else int(x['tmja_tv_x']/x['tmja_tv_y']*x['tmjo_2_sens_y']),
                                                      axis=1)
    #si plusieurs resultas possibles pour unident on garde le max
    trafc_fin=trafc_rens.loc[trafc_rens['traf_max']==trafc_rens.groupby('ident_a_rens').traf_max.transform(max)].merge(
        df_tot[['idtronc','ident']], left_on='ident_a_rens',right_on='ident')[['ident_a_rens','tmjo_2_sens_extrapol','idtronc']].drop_duplicates()
    
    return trafc_fin
    
    
    

class PasCorrespondanceError(Exception):
    """
    Exception levée si la ligne rhv n'est pas associee a une ligne MMM
    """
    def __init__(self, ident_rhv):
        Exception.__init__(self,f'pas de correspondance MMM pour la(es) ligne : {ident_rhv}')
        
        

