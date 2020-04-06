# -*- coding: utf-8 -*-
'''
Created on 20 déc. 2019

@author: martin.schoreisz

simplifier les ronds points dans un graph, mais conserver les géométries de départ
'''

import pandas as pd
from Base_BdTopo.Import_outils import import_donnes_base
from Base_BdTopo.Rond_points import identifier_rd_pt

def identifier_ronds_points(bdd, schema, graph_ligne, graph_vertex):
    """
    identifier les rd pt du jeu dde données en se basant sur le travail interne OTV
    """
    df=import_donnes_base(bdd, schema, graph_ligne, graph_vertex)
    df_avec_rd_pt=identifier_rd_pt(df)[0]
    lgn_rdpt=df_avec_rd_pt.loc[~df_avec_rd_pt.id_rdpt.isna()].copy()
    return df_avec_rd_pt, lgn_rdpt

def creer_dico_noeud_rdpt(lgn_rdpt):
    """
    creer un dico avec comme cle le noeud synthetique et comme valeur tout les noeud d'un rd pt
    in : 
        lgn_rdpt : df des lignes concernees par un rd point, doit contenir les attributs 'source', 'target' et 'id_rdpt'
    out : 
        dico_noeud: dico des noeuds permettantde simplifier les rd points
    """
    noeud_rdpt=list(set(lgn_rdpt.source.tolist()+lgn_rdpt.target.tolist()))
    dico_noeud={}
    while noeud_rdpt :
        rdpt_id=lgn_rdpt.loc[(lgn_rdpt['source']==noeud_rdpt[0]) | (lgn_rdpt['target']==noeud_rdpt[0])].id_rdpt.values[0]
        ts_noeud=lgn_rdpt.loc[lgn_rdpt['id_rdpt']==rdpt_id].source.tolist()+lgn_rdpt.loc[lgn_rdpt['id_rdpt']==rdpt_id].target.tolist()
        dico_noeud[ts_noeud[0]]=[a for a in set(ts_noeud)]
        noeud_rdpt=[a for a in noeud_rdpt if a not in [b for a in dico_noeud.values() for b in a]]
    return dico_noeud

def simplifier_noeud_rdpt(df, dico_noeud):
    """
    affecter la mm valeur de noeud à toute les lignes arrivant sur un rd point. fonctionne en modifiant sur place
    in : 
        df : dataframe des lignes. doit contenir 'source' et 'target', ne doit pas contenir les lignes constituant les rdpoints
        dico_noeud: dico des noeuds permettantde simplifier les rd points, issu de creer_dico_noeud_rdpt()
    out : 
        df_simplifie : df avec source et target modifie
    """
    #fonction de remplacement
    def remplace_rdpt(dico_noeud, noeud) : 
        for k, v in dico_noeud.items() : 
            if noeud in v : 
                return k
        else : return noeud
    df['source']=df.source.apply(lambda x : remplace_rdpt(dico_noeud, x))
    df['target']=df.target.apply(lambda x : remplace_rdpt(dico_noeud, x))
    return 

def maj_graph_rdpt(df):
    """
    produire une valuer de graph sans geometrie (colonne id, cnt uniquement)
    in : 
        df : dataframe des lignes modifie en source et target, issu de simplifier_noeud_rdpt()
    out: 
        cnt_maj : dataframe avec l'id du noeu det le nombre de ligne qui le touchent
    """
    cnt_maj=pd.concat([df[['source']].rename(columns={'source':'id'}),
                       df[['target']].rename(columns={'target':'id'})], 
                   axis=0, sort=False).reset_index().groupby('id').count().reset_index().rename(columns={'ident':'cnt'})
    return cnt_maj

def donnees_tot_rd_pt(gdf_base, bdd, schema, graph_ligne, graph_vertex):
    """
    dans une gdf de base, reaffacter source ou target avec un seul numero, pour tout les vertex relatif au mm rdpt    
    in : 
        gdf_base : données de référentiel (normalement le rhv)
        bdd : bdd dans laquelle est stocké&e le référentiel cf travail interne OTV
        schema : schema dans lequel est stocké&e le référentiel cf travail interne OTV
        graph_ligne : référentiel avec ajout des attributs source et target  cf travail interne OTV
        graph_vertex : count des noeuds du référentiel avec ajout des attributs source et target  cf travail interne OTV
        dico_noeud : dico avec en cle une valeur de noeud et en value la list de tout les noeuds correspondants     
    """
    lgn_rdpt=identifier_ronds_points(bdd, schema, graph_ligne, graph_vertex)[1]
    dico_noeud=creer_dico_noeud_rdpt(lgn_rdpt)
    gdf_rhv_rdpt_simple=gdf_base.loc[~gdf_base.ident.isin(lgn_rdpt.ident.to_list())].copy()
    #remplacement des sources et targets : 
    simplifier_noeud_rdpt(gdf_rhv_rdpt_simple, dico_noeud)
    return lgn_rdpt, dico_noeud, gdf_rhv_rdpt_simple
    
    
    