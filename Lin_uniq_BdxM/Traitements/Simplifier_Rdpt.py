# -*- coding: utf-8 -*-
'''
Created on 20 déc. 2019

@author: martin.schoreisz

simplifier les ronds points dans un graph, mais conserver les géométries de départ
'''

import pandas as pd

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