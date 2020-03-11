# -*- coding: utf-8 -*-
'''
Created on 28 janv. 2020

@author: martin.schoreisz
Affectation des points de comptages au RHV 
'''

import pandas as pd
from Outils import plus_proche_voisin

def carac_lgn_proche_perm(cpt_perm_final,gdf_rhv_groupe,distance):
    """
    trouver la ligne la plus proche du comptage permanent
    in :
       cpt_perm_final : df  des pt de comptage avec un id de groupe. issu d'un fichier shp valilde
       gdf_rhv_groupe : df : groupement des troncon du rhv par tronocn elementaire, toute categorie confondu
       distance : integer : distance max à l'objet le plus proche
    """
    lgn_proche_perm=cpt_perm_final.merge(plus_proche_voisin(cpt_perm_final,gdf_rhv_groupe,10,'ident','ident'),left_on='ident', right_on='ident_left',how='left').merge(
                            gdf_rhv_groupe[['ident','cat_rhv','rgraph_dbl','idtronc']], left_on='ident_right', right_on='ident', how='left').rename(
                                columns={'ident_right':'ident_lgn'})
    lgn_proche_attr_sup=lgn_proche_perm.merge(lgn_proche_perm.groupby('id_grp')['gid'].nunique(), left_on='id_grp',
                    right_index=True).rename(columns={'gid_y':'nb_cpt'})
    return lgn_proche_attr_sup

def tmjo_2sens_perm(lgn_proche_attr_sup):
    """
    calcul du tmjo tout sens pour les compteur permanents
    in : 
       lgn_proche_attr_sup : df des lignes proches de comptages, issu de  carac_lgn_proche_perm()
    """
    dico_verif_nb_sens={'Z27CT9':2,'Z27CT6':2,'Z31CT7':2,'Z31CT8':2,'Z25CT2':2,'Z23CT1':2,'Z25CT6':2,'Z14CT12':2,'Z14CT7':2,'Z14CT10':2,
                   'Z14CT17':2,'Z14CT16':2,'Z1CT14':2,'Z1CT16':2,'Z1CT1':2,'Z1CT3' : 2,'Z1CT13':2,'Z5CT2':2,'Z29CT11':2,'Z8CT11':2,'Z8CT4':2,
                   'Z8CT5':2,'Z8CT8':2,'Z22CT8':2,'Z29CT1':2,'Z30CT9':2,'Z13CT2':2,'Z9CT2':2,'Z11CT14':2,'Z11CT10':2,'Z26CT1':2,'Z11CT16':2,'Z17CT14':2,
                   'Z17CT3': 2,'Z17CT9':2,'Z17CT4':2,'Z16CT14':2,'Z9CT15' :2,'Z8CT2': 2, 'Z7CT5':2,'0017.93_1':2,
                   'Z3CT18':2, 'Z2CT11':2,'Z2CT5':2, 'Z13CT10' : 1.5}
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
    
    lgn_proche_attr_sup['tmjo_2_sens']=lgn_proche_attr_sup.apply(lambda x : calcul_tmjo_2sens_perm(x['mjo_val'],x['rgraph_dbl'],
                                                                                          x['nb_cpt'], lgn_proche_attr_sup,x['id_grp'],
                                                                                          'mjo_val','id_grp',x['ident_x'],'ident_x')[0],axis=1)
    lgn_proche_attr_sup['id_cpt_2_sens']=lgn_proche_attr_sup.apply(lambda x : calcul_tmjo_2sens_perm(x['mjo_val'],x['rgraph_dbl'],
                                                                                          x['nb_cpt'], lgn_proche_attr_sup,x['id_grp'],
                                                                                          'mjo_val','id_grp',x['ident_x'],'ident_x')[1],axis=1)    
    

