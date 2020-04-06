# -*- coding: utf-8 -*-
'''
Created on 28 janv. 2020

@author: martin.schoreisz
Affectation des points de comptages au RHV 
'''

import pandas as pd
from Outils import plus_proche_voisin

"""##########################
COMPTAGES PERMANENT
############################"""

def carac_lgn_proche_perm(cpt_perm_final,gdf_rhv_groupe,distance):
    """
    trouver la ligne la plus proche du comptage permanent
    in :
       cpt_perm_final : df  des pt de comptage avec un id de groupe. issu d'un fichier shp valilde
       gdf_rhv_groupe : df : groupement des troncon du rhv par tronocn elementaire, toute categorie confondu
       distance : integer : distance max à l'objet le plus proche
    """
    lgn_proche_perm=cpt_perm_final.merge(plus_proche_voisin(cpt_perm_final,gdf_rhv_groupe,distance,'ident','ident'),left_on='ident', right_on='ident_left',how='left').merge(
                            gdf_rhv_groupe[['ident','cat_rhv','rgraph_dbl','idtronc']], left_on='ident_right', right_on='ident', how='left').rename(
                                columns={'ident_right':'ident_lgn'})
    lgn_proche_attr_sup=lgn_proche_perm.merge(lgn_proche_perm.groupby('id_grp')['gid'].nunique(), left_on='id_grp',
                    right_index=True).rename(columns={'gid_y':'nb_cpt'})
    return lgn_proche_attr_sup, lgn_proche_perm

def tmjo_2sens_perm_v0(lgn_proche_attr_sup,gdf_rhv_groupe_123):
    """
    calcul du tmjo tout sens pour les compteur permanents, affectation aux troncons des cat 1,2,3
    in : 
       lgn_proche_attr_sup : df des lignes proches de comptages, issu de  carac_lgn_proche_perm()
       gdf_rhv_groupe_123 : groupement des troncon du rhv par tronocn elementaire, categorie 1,2,3 uniquement
    """
    dico_verif_nb_sens={'Z27CT9':2,'Z27CT6':2,'Z31CT7':2,'Z31CT8':2,'Z25CT2':2,'Z23CT1':2,'Z25CT6':2,'Z14CT12':2,'Z14CT7':2,'Z14CT10':2,
                   'Z14CT17':2,'Z14CT16':2,'Z1CT14':2,'Z1CT16':2,'Z1CT1':2,'Z1CT3' : 2,'Z1CT13':2,'Z5CT2':2,'Z29CT11':2,'Z8CT11':2,'Z8CT4':2,
                   'Z8CT5':2,'Z8CT8':2,'Z22CT8':2,'Z29CT1':2,'Z30CT9':2,'Z13CT2':2,'Z9CT2':2,'Z11CT14':2,'Z11CT10':2,'Z26CT1':2,'Z11CT16':2,'Z17CT14':2,
                   'Z17CT3': 2,'Z17CT9':2,'Z17CT4':2,'Z16CT14':2,'Z9CT15' :2,'Z8CT2': 2, 'Z7CT5':2,'0017.93_1':2,
                   'Z3CT18':2, 'Z2CT11':2,'Z2CT5':2,'Z8CT13':2, 'Z13CT10' : 1.5}
    def calcul_tmjo_2sens_perm(tmjo, rgraph_dbl, nb_cpt, df,id_grp,nom_attr_trafic,nom_attr_id_grp,id_cpt,nom_attr_id_cpt,) :
        """calculer le tmja 2 sens en fonction du nb de compteur et du sens unique ou non"""
        if nb_cpt==2 : 
            return df.loc[df[nom_attr_id_grp]==id_grp][nom_attr_trafic].sum(), df.loc[df[nom_attr_id_grp]==id_grp].groupby(nom_attr_id_grp)[nom_attr_id_cpt].agg(
                lambda x : tuple(x)).values[0]
        else : 
            if id_cpt in dico_verif_nb_sens.keys() : 
                return tmjo*dico_verif_nb_sens[id_cpt], (id_cpt,)
            elif rgraph_dbl==1 : 
                return tmjo*2, (id_cpt,)
            else :
                return tmjo, (id_cpt,)
    
    #calcul
    lgn_proche_attr_sup['tmjo_2_sens']=lgn_proche_attr_sup.apply(lambda x : calcul_tmjo_2sens_perm(x['mjo_val'],x['rgraph_dbl'],
                                                                                          x['nb_cpt'], lgn_proche_attr_sup,x['id_grp'],
                                                                                          'mjo_val','id_grp',x['ident_x'],'ident_x')[0],axis=1)
    lgn_proche_attr_sup['id_cpt_2_sens']=lgn_proche_attr_sup.apply(lambda x : calcul_tmjo_2sens_perm(x['mjo_val'],x['rgraph_dbl'],
                                                                                          x['nb_cpt'], lgn_proche_attr_sup,x['id_grp'],
                                                                                          'mjo_val','id_grp',x['ident_x'],'ident_x')[1],axis=1)    
    #lien vers l'idtronc des cat 1,2,3
    mjo_tronc_cat123=lgn_proche_attr_sup.loc[lgn_proche_attr_sup['cat_rhv'].isin(['1','2','3','61','62','63'])][['ident_lgn','tmjo_2_sens','id_cpt_2_sens']].merge(
                        gdf_rhv_groupe_123[['ident','idtronc']].rename(columns={'ident':'ident_lgn'}),how='left')#[['idtronc','tmjo_2_sens']]
    mjo_tronc_cat123=mjo_tronc_cat123.drop_duplicates(['idtronc','tmjo_2_sens'])
    return mjo_tronc_cat123
    
def nettoyage_dbl_tmjo_2_sens_perm(mjo_tronc_cat123):
    """
    nettoyer les données de tmjo_2sens_perm_v0 (doublons )
    in :
        mjo_tronc_cat123 : df des données de comptage par idtronc sur les cat 1,2,3 issue de tmjo_2sens_perm_v0
    """
    traf_max=mjo_tronc_cat123.loc[mjo_tronc_cat123.duplicated('idtronc',keep=False)].groupby('idtronc').tmjo_2_sens.max().reset_index().merge(
    mjo_tronc_cat123, on='idtronc')
    traf_max=traf_max.loc[traf_max['tmjo_2_sens_x']==traf_max['tmjo_2_sens_y']].drop('tmjo_2_sens_y',axis=1).rename(columns={'tmjo_2_sens_x':'tmjo_2_sens'}).set_index('idtronc').copy()
    mjo_tronc_cat123.set_index('idtronc',inplace=True)
    mjo_tronc_cat123.update(traf_max.reset_index().set_index('idtronc'))
    mjo_tronc_cat123.reset_index(inplace=True)
    mjo_tronc_cat123_nettoye=mjo_tronc_cat123.drop_duplicates(['idtronc','tmjo_2_sens'])
    return mjo_tronc_cat123_nettoye

def propagation_cpt_perm(gdf_rhv_groupe_123, mjo_tronc_cat123_nettoye):
    """
    affectation des trafic 2 sens des competurs perm vers les lignes du rhv selon l'idtronc des cat 1,2,3
    in : 
        gdf_rhv_groupe_123 : groupement des troncon du rhv par tronocn elementaire, categorie 1,2,3 uniquement
        mjo_tronc_cat123_nettoye : données de trafic par troncon, issu de nettoyage_dbl_tmjo_2_sens_perm
    """
    #jointure entre l'idtronc issu des cat 1,2,3 et les lignes qui y sont affectées puis jointure avec la df de base des lignes
    gdf_rhv_cpt_perm_123=gdf_rhv_groupe_123.merge(mjo_tronc_cat123_nettoye, on='idtronc', how='left')
    gdf_rhv_cpt_perm_123.loc[~gdf_rhv_cpt_perm_123.tmjo_2_sens.isna(),'type_cpt']='permanent'
    gdf_rhv_cpt_perm_123.drop('ident_lgn',axis=1, inplace=True)
    return gdf_rhv_cpt_perm_123

def affectation_cpt_perm(cpt_perm_final,gdf_rhv_groupe,gdf_rhv_groupe_123,distance):
    """
    fonctio d'assemblage des fonctions carac_lgn_proche_perm, tmjo_2sens_perm_v0, nettoyage_dbl_tmjo_2_sens_perm, propagation_cpt_perm
    """
    lgn_proche_attr_sup, lgn_proche_perm=carac_lgn_proche_perm(cpt_perm_final,gdf_rhv_groupe,distance)
    mjo_tronc_cat123=tmjo_2sens_perm_v0(lgn_proche_attr_sup,gdf_rhv_groupe_123)
    mjo_tronc_cat123_nettoye=nettoyage_dbl_tmjo_2_sens_perm(mjo_tronc_cat123)
    gdf_rhv_cpt_perm_123=propagation_cpt_perm(gdf_rhv_groupe_123, mjo_tronc_cat123_nettoye)
    return gdf_rhv_cpt_perm_123, lgn_proche_perm

def export_cpt_perm_linearises(gdf_rhv_cpt_perm_123,fichier):
    """
    exporter en shp le fichier cree par affectation_cpt_perm
    """
    gdf_rhv_cpt_perm_123['id_cpt_exp']=gdf_rhv_cpt_perm_123['id_cpt_2_sens'].fillna('NC')
    gdf_rhv_cpt_perm_123['id_cpt_exp']=gdf_rhv_cpt_perm_123.apply(lambda x : ', '.join([str(a) for a in x['id_cpt_exp']]) 
                    , axis=1)
    gdf_rhv_cpt_perm_123[['id_x', 'ident', 'domanial', 'groupe', 'cat_dig', 'cat_rhv', 'passage',
           'rggraph_nd', 'rggraph_na', 'rgraph_dbl', 'numero', 'cdate', 'mdate',
           'id_ign', 'nature', 'sens', 'codevoie_d', 'importance', 'id_y','source', 'target',
           'idtronc', 'geometry','tmjo_2_sens', 'type_cpt','id_cpt_exp']].to_file(fichier)

"""##########################
COMPTAGES PONCTUELS
############################"""

def carac_lgn_proche_ponct_ok(cpt_pct_l93, affect_finale, lgn_proche_perm, gdf_rhv_groupe, distance):
    """
    filtrer le spoint de comptage ponctuels, chercher les points qui supporte pas déjà un tafic d'un comptage permanent (attention, on se base sur les troncon elementaires toute
    catégorie rhv).
    Ensuite trouver la ligne du rhv la plus proche
    in : 
        cpt_pct_l93 : df des points de comptages ponctuels, issu du module Comptage, fonction traitements_bdxm_pct()
        affect_finale : df de regroupement des comptages ponctuels, issu du module Comptage, fonction traitements_bdxm_pct()
    """
    cpt_ponct_ok=cpt_pct_l93.merge(affect_finale.loc[~affect_finale.idtronc.isin(lgn_proche_perm.idtronc.tolist())][['ident','idtronc']], on='ident')
    lgn_proche_ponct=cpt_ponct_ok.merge(plus_proche_voisin(cpt_ponct_ok,gdf_rhv_groupe,distance,'ident','ident'),left_on='ident', right_on='ident_left',how='left').merge(
        gdf_rhv_groupe[['ident','cat_rhv','rgraph_dbl','idtronc']], left_on='ident_right', right_on='ident', how='left').rename(
        columns={'ident_right':'ident_lgn','idtronc_x':'idtronc_tt_rhv','ident_x':'ident'}).drop(['ident_left','ident_y','idtronc_y'],axis=1)
    return cpt_ponct_ok, lgn_proche_ponct

def tmjo_2sens_ponct_v0(lgn_proche_ponct):
    """
    calcul du tmjo tout sens pour les compteur ponctuels
    in : 
       lgn_proche_ponct : df des lignes proches de comptages, issu de  carac_lgn_proche_ponct_ok()
       gdf_rhv_groupe_123 : groupement des troncon du rhv par tronocn elementaire, categorie 1,2,3 uniquement
    """
    
    #calculer le trafic total par point de comptage ponctuel
    def calcul_tmjo_2sens_ponct(tmjo, rgraph_dbl, nb_cpt,sens_uniq, df,id_grp,nom_attr_trafic,nom_attr_id_grp,id_cpt,nom_attr_id_cpt) :
        """calculer le tmja 2 sens en fonction du nb de compteur et du sens unique ou non"""
        if nb_cpt==2 : 
            if (df.loc[df[nom_attr_id_grp]==id_grp].sens_unique==True).all() : #si sur unmm idtronc les 2 copt sont en sens uniq, on garde le max
                return (df.loc[df[nom_attr_id_grp]==id_grp][nom_attr_trafic].max(),df.loc[(df[nom_attr_id_grp]==id_grp) & (df[nom_attr_trafic]==
                            df.loc[df[nom_attr_id_grp]==id_grp][nom_attr_trafic].max())][nom_attr_id_cpt].values[0],)
            else :
                return df.loc[df[nom_attr_id_grp]==id_grp][nom_attr_trafic].sum(), df.loc[df[nom_attr_id_grp]==id_grp].groupby(nom_attr_id_grp)[nom_attr_id_cpt].agg(
                    lambda x : tuple(x)).values[0]
        else : 
            if rgraph_dbl==1 : 
                return tmjo*2, (id_cpt,)
            else : 
                return tmjo, (id_cpt,)
    
    lgn_proche_ponct_attr_sup=lgn_proche_ponct.merge(lgn_proche_ponct.groupby('idtronc_tt_rhv')['ident'].nunique(), on='idtronc_tt_rhv').rename(
                                columns={'ident_y':'nb_cpt','ident_x':'ident'})
    lgn_proche_ponct_attr_sup['tmjo_2_sens']=lgn_proche_ponct_attr_sup.apply(lambda x : 
                                            calcul_tmjo_2sens_ponct(x['tmjo_tv'],x['rgraph_dbl'],x['nb_cpt'],x['sens_unique'], lgn_proche_ponct_attr_sup,
                                            x['idtronc_tt_rhv'],'tmjo_tv','idtronc_tt_rhv',x['ident'],'ident')[0],axis=1)
    lgn_proche_ponct_attr_sup['id_cpt_2_sens']=lgn_proche_ponct_attr_sup.apply(lambda x : 
                                               calcul_tmjo_2sens_ponct(x['tmjo_tv'],x['rgraph_dbl'],x['nb_cpt'],x['sens_unique'], lgn_proche_ponct_attr_sup,
                                               x['idtronc_tt_rhv'],'tmjo_tv','idtronc_tt_rhv',x['ident'],'ident')[1],axis=1)
    return lgn_proche_ponct_attr_sup

def classer_cpt_ponct_dbl(lgn_proche_ponct_attr_sup,gdf_rhv_cpt_perm_123,gdf_rhv_groupe_123):
    """
    separer les comptages ponctuels, en deux catégories : ceux qui sont sur des troncons (de cat 1,2,3) relatif à un comptage permanent et les autres.
    Dans les autres, re-séparé en 2 catégorie: sont qui sont en doublons, et ceux qui sont utilisables tel quel
    permet aussi de connaitre la liste des comptages ponctuels relatifs à un comptages permanents, ou ceux en doublons, ou les libres
    in :
       lgn_proche_ponct_attr_sup : df des cpt ponct avec le tmjo 2sens   issu de tmjo_2sens_ponct_v0
       gdf_rhv_cpt_perm_123 : df des lignes du rhv de cat 1,2,3, avec en plus le tmjo issu des comptages permanent. iisu de affectation_cpt_perm
       gdf_rhv_groupe_123 : groupement des troncon du rhv par tronocn elementaire, categorie 1,2,3 uniquement
    out : 
        ponct_sur_perm_123 : df des cpt ponct situé sur le mm tronc elem cat 1,2,3 qu'un comptage perm
        ponct_libre_123 : df des cpt ponct non relatif à un comptage perm
        ponct_libre_123_tot : df des idtronc cat 1,2,3 avec le tmjo et les comptages ponctuels associés. contient les idtronc avec et sans doublons
        list_pct_libre_dbl : liste des idtronc de cat 1,2,3 avec plusieurs comptg ponctuels
        pct_libre_123_avec_dbl : df idtronc de cat 1,2,3 cavec plsr comptage ponctuels
        idtronc123_pct_sur_perm : df des id tronc supportant des ponctuels et permanents
    """
    ponct_sur_perm_123=lgn_proche_ponct_attr_sup.loc[lgn_proche_ponct_attr_sup['ident_lgn'].isin(
                        gdf_rhv_cpt_perm_123.loc[gdf_rhv_cpt_perm_123['type_cpt']=='permanent'].ident.tolist())].copy()
    ponct_libre_123=lgn_proche_ponct_attr_sup.loc[~lgn_proche_ponct_attr_sup['ident_lgn'].isin(
                    gdf_rhv_cpt_perm_123.loc[gdf_rhv_cpt_perm_123['type_cpt']=='permanent'].ident.tolist())].copy()
    
    #df des id tronc supportant des ponctuels et permanents
    idtronc123_pct_sur_perm=gdf_rhv_groupe_123[['ident','idtronc']].merge(ponct_sur_perm_123[['ident_lgn','tmjo_2_sens','id_cpt_2_sens']].rename(columns={'ident_lgn':'ident'}), 
                                                                          on='ident')[['idtronc','tmjo_2_sens','id_cpt_2_sens']].drop_duplicates()
    
    #rappatriement des compteurs sur les idtronc des cat 1,2,3 : il y a des doublons car les troncons sont long, donc on separe, et on va traiter les ponct_libr sans doublons
    ponct_libre_123_tot=gdf_rhv_groupe_123[['ident','idtronc']].merge(ponct_libre_123[['ident_lgn','tmjo_2_sens','id_cpt_2_sens']].rename(columns={'ident_lgn':'ident'}), 
                                on='ident')[['idtronc','tmjo_2_sens','id_cpt_2_sens']].drop_duplicates(['idtronc','tmjo_2_sens','id_cpt_2_sens'])
    #liste des ponctuels non relatig a unpermanent mais en doublons avec d'autres ponctuels
    list_pct_libre_dbl=ponct_libre_123_tot.loc[ponct_libre_123_tot.duplicated('idtronc',keep=False)].sort_values('idtronc').idtronc.unique()
    pct_libre_123_avec_dbl=ponct_libre_123_tot.loc[ponct_libre_123_tot['idtronc'].isin(list_pct_libre_dbl)].copy()
    return ponct_libre_123, ponct_libre_123_tot, list_pct_libre_dbl, pct_libre_123_avec_dbl, idtronc123_pct_sur_perm

def affecter_cpt_ponct_libre(ponct_libre_123_tot,list_pct_libre_dbl,gdf_rhv_cpt_perm_123):
    """
    affectation des ponctuels libre sans doublons à l'ensemble des lignes d'un idtronc cat 1,2,3
    in :
        ponct_libre_123_tot : df des comptage ponctuels ramené sur un idtronc des cat 1,2,3. issu de classer_cpt_ponct_dbl
        list_pct_libre_dbl : liste des idtronc de cat 1,2,3 ayant plusieurs comptg ponctuels. classer_cpt_ponct_dbl
        gdf_rhv_cpt_perm_123 : df des lignes du rhv de cat 1,2,3, avec en plus le tmjo issu des comptages permanent. iisu de affectation_cpt_perm
    """
    pct_libre_123_ss_dbl=ponct_libre_123_tot.loc[~ponct_libre_123_tot['idtronc'].isin(list_pct_libre_dbl)].copy()
    pct_libre_123_ss_dbl['type_cpt']='ponctuel'
    gdf_traf_pct_libre=gdf_rhv_cpt_perm_123.set_index('idtronc')
    gdf_traf_pct_libre.update(pct_libre_123_ss_dbl.set_index('idtronc'))
    gdf_traf_pct_libre.reset_index(inplace=True)
    return gdf_traf_pct_libre

def affecter_cpt_ponct_dbl(list_pct_libre_dbl,lgn_proche_ponct_attr_sup,pct_libre_123_avec_dbl,gdf_traf_pct_libre):
    """
    affectation du trafic des ponctuel libre avec doublons aux troncons cat 1,2,3.
    on affecte à l'id_tronc le max des compteurs en doublons
    in : 
        list_pct_libre_dbl : liste des idtronc de cat 1,2,3 ayant plusieurs comptg ponctuels issu de classer_cpt_ponct_dbl
        lgn_proche_ponct_attr_sup : df des lignes proches de comptages ponctuels, avec tmj. issu de tmjo_2sens_ponct_v0
        pct_libre_123_avec_dbl : df idtronc de cat 1,2,3 cavec plsr comptage ponctuels. issu de classer_cpt_ponct_dbl
        gdf_traf_pct_libre : df des lignes du rhv, deja renseigne pour les comptages perm et ponct sans doublons. issu de affecter_cpt_ponct_libre
    """
    gdf_traf_pct_libre=gdf_traf_pct_libre.reset_index().set_index('ident')
    for idtronc_test in list_pct_libre_dbl : 
        #récupérer l'idtronc toute cat_rhv des compteurs avec doublons et faire la jointure du trafic sur les lignes concernees
        pct_libre_123_avec_dbl_decompose=lgn_proche_ponct_attr_sup.loc[lgn_proche_ponct_attr_sup['ident'].isin(
            [a for b in pct_libre_123_avec_dbl.loc[pct_libre_123_avec_dbl['idtronc']==idtronc_test].
             id_cpt_2_sens.tolist() for a in b])][['idtronc_tt_rhv','tmjo_2_sens','id_cpt_2_sens']].copy()
        traf_max=pct_libre_123_avec_dbl_decompose.tmjo_2_sens.max()
        id_cpt=pct_libre_123_avec_dbl_decompose.loc[pct_libre_123_avec_dbl_decompose['tmjo_2_sens']==traf_max].id_cpt_2_sens.unique()[0]
        gdf_traf_pct_libre.loc[gdf_traf_pct_libre['idtronc']==idtronc_test,'tmjo_2_sens']=traf_max
        gdf_traf_pct_libre.loc[gdf_traf_pct_libre['idtronc']==idtronc_test,'id_cpt_2_sens']=','.join([str(a) for a in id_cpt])
        gdf_traf_pct_libre.loc[gdf_traf_pct_libre['idtronc']==idtronc_test,'type_cpt']='ponctuel'
    gdf_traf_pct_libre.reset_index(inplace=True)
    return gdf_traf_pct_libre

def affectation_cpt_ponct(cpt_pct_l93, affect_finale, lgn_proche_perm, gdf_rhv_groupe, distance, gdf_rhv_groupe_123, gdf_rhv_cpt_perm_123) : 
    """
    fonction d'assemblage des fonction de calcul du tmjo des comptages pocntuels
    """
    lgn_proche_ponct=carac_lgn_proche_ponct_ok(cpt_pct_l93, affect_finale, lgn_proche_perm, gdf_rhv_groupe, distance)[1]
    lgn_proche_ponct_attr_sup=tmjo_2sens_ponct_v0(lgn_proche_ponct)
    ponct_libre_123_tot, list_pct_libre_dbl, pct_libre_123_avec_dbl=classer_cpt_ponct_dbl(lgn_proche_ponct_attr_sup,gdf_rhv_cpt_perm_123,gdf_rhv_groupe_123)[1:4]
    gdf_traf_pct_libre=affecter_cpt_ponct_libre(ponct_libre_123_tot,list_pct_libre_dbl,gdf_rhv_cpt_perm_123)
    gdf_traf_tot=affecter_cpt_ponct_dbl(list_pct_libre_dbl,lgn_proche_ponct_attr_sup,pct_libre_123_avec_dbl,gdf_traf_pct_libre)
    return gdf_traf_tot
    
def export_cpt_ponct_linearises(gdf_traf_tot,fichier) : 
    gdf_traf_tot['id_cpt_exp']=gdf_traf_tot['id_cpt_2_sens'].fillna('NC')
    gdf_traf_tot['id_cpt_exp']=gdf_traf_tot.apply(lambda x : ', '.join([str(a) for a in x['id_cpt_exp']]) 
                    if isinstance(x['id_cpt_2_sens'],tuple) else str(x['id_cpt_2_sens']), axis=1)
    gdf_traf_tot.reset_index()[['id_x', 'ident', 'domanial', 'groupe', 'cat_dig', 'cat_rhv', 'passage',
           'rggraph_nd', 'rggraph_na', 'rgraph_dbl', 'numero', 'cdate', 'mdate',
           'id_ign', 'nature', 'sens', 'codevoie_d', 'importance', 'id_y','source', 'target',
           'idtronc', 'geometry','tmjo_2_sens','type_cpt', 'id_cpt_exp']].to_file(fichier)

    

