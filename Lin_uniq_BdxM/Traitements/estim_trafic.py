# -*- coding: utf-8 -*-
'''
Created on 16 dec. 2019

@author: martin.schoreisz

Module de ventilation des trafics issus des points de comptages
'''

import pandas as pd
from Outils import plus_proche_voisin,nb_noeud_unique_troncon_continu
from collections import Counter

def noeuds_estimables(df_trafic_pt_comptg):
    """
    trouver les noeuds pouvant etre estime, i.e avec au moins 1 valuer de trafic pour les toncons qui arrivent.
    in : 
        df_trafic_pt_comptg : df des lignes fv avec simplification des ronds points et trafics des points de compatges
    """
    #1. faire la liste des lignes et des noeuds
    df_noeuds=pd.concat([df_trafic_pt_comptg[['id_ign','source','tmjo_2_sens','cat_rhv','rgraph_dbl']].rename(columns={'source':'noeud'}),
                            df_trafic_pt_comptg[['id_ign','target','tmjo_2_sens','cat_rhv','rgraph_dbl']].rename(columns={'target':'noeud'})],axis=0,sort=False)
    df_noeuds.tmjo_2_sens.fillna(-99,inplace=True)
    #2. Trouver les noeuds qui comporte au moins une valeur connue. On y affecte une valuer True dans un attribut drapeau 'estimable'
    noeud_grp=df_noeuds.groupby('noeud').agg({'tmjo_2_sens':lambda x : tuple(x),'cat_rhv':lambda x : tuple(x)}).reset_index()
    noeud_grp['nb_nan']=noeud_grp.tmjo_2_sens.apply(lambda x : Counter(x))
    noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['nb_nan'])>2 and x['nb_nan'][-99]<len(x['nb_nan']) and x['nb_nan'][-99]!=0 else False,axis=1)
    noeud_estimable=noeud_grp.loc[noeud_grp['estimable']].copy()
    return noeud_estimable,noeud_grp, df_noeuds

def df_noeud_troncon(df_tot, noeud,df_noeud_tot):
    """
    determiner la dataframe des troncon en rapport avec un noeud, en se basant surla ligne du troncon concerne
    in : 
        df_tot : dataframe des lignes contenant un idtronc, et ayant été modifié au niveau de noeud de rdpt (Simplifier_rdpt.maj_graph_rdpt())
        noeud : integer : numero du noeud
        df_noeud_tot : dataframe de l'ensemnle des noeuds, issus de noeud_fv_ligne_ss_trafic()
    out : 
        df_noeud : dataframe des tronc arrivant sur le noeud avec id_ign,noeud, tmjo_2_sens,cat_rhv,type_noeud,idtronc
    """
    df_temp=df_tot.loc[(df_tot['source']==noeud) | (df_tot['target']==noeud)].copy()
    df_temp['type_noeud']=df_temp.apply(lambda x : 'd' if x['source']==noeud else 'a', axis=1 )
    df_noeud=df_noeud_tot.loc[df_noeud_tot['noeud']==noeud].merge(df_temp[['id_ign','type_noeud', 'idtronc']], on='id_ign')
    return df_noeud

def verif_double_sens(df, df_tot):
    """
    Vérifier si un troncon est en double sens ou non
    in : 
        df : dataframe des troncons relatifs à un noeud. issu de df_noeud_troncon()
        df_tot : dataframe des lignes, sans modification des sources et target des rond points
    """
    
    def nb_sens(rgraph_dbl,nb_noeud_unique,nb_noeud,nb_lgn_tch_noeud_unique,nb_ligne) : 
        """
        savoir si un troncon est en double sens ou non en fonction du nb de noeud, noeuds_uniques, lignes et lignes
        qui touchent les noeuds uniques
        """
        if rgraph_dbl==0 :
            if nb_noeud_unique > 2 :
                if nb_lgn_tch_noeud_unique > 2 : 
                    if nb_ligne==nb_noeud-1 : 
                        return 0
                    elif nb_ligne==nb_noeud-2 : 
                        return 1
                elif nb_lgn_tch_noeud_unique == 2 :
                    return 1
            else : return 0
        else: return 1
    
    df['list_noeud_unique']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).noeuds_uniques, axis=1) #liste des noeuds en fin de troncon['list_noeud_unique']=df_calcul.apply(lambda x : rt.Troncon(df_tot, x['idtronc']).noeuds_uniques, axis=1) #liste des noeuds en fin de troncon
    df['nb_noeud_unique']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_noeuds_uniques, axis=1) #nb de noeud en fin de troncon
    df['nb_noeud']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_noeuds, axis=1) #nb de noeud en fin de troncon
    df['nb_lgn_tch_noeud_unique']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_lgn_tch_noeud_unique, axis=1) 
    df['nb_lgn']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_lgn, axis=1)
    #ajout d'un attribut supplémentaire
    df['rgraph_dbl_2']=df.apply(lambda x : nb_sens(x['rgraph_dbl'],x['nb_noeud_unique'],x['nb_noeud'],x['nb_lgn_tch_noeud_unique'],x['nb_lgn']), axis=1)

def carac_troncon_noeud(df_rdpt_simple, df_tot, graph,num_noeud,df_noeuds):
    """
    verifier que les voies représetées par 2 lignes ne croise bien que 2 autres troncons. 
    Cette vérif est due à l'analyse par noeud : pour une 2*2 voies il y a 4 noeuds de fin, dc on pourrait louper des lignes
    in : 
        df_rdpt_simple : ataframe du filaire de voie avec rond point simplifie et idtroncon
        df_tot : df du filaire sans modif des ronds points
        graoh : table des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
        num_noeud : integer : numero du noeud du carrefour
        df_noeuds : df de tout les oeuds, issu de noeuds_estimables()
    out : 
        df_tronc_finale : df des tronncon arrivant sur un noeud, toutes voies confodues (prise en compte 2*2 voies)
    """
    df_troncon_noeud=df_noeud_troncon(df_rdpt_simple, num_noeud,df_noeuds)
    verif_double_sens(df_troncon_noeud,df_tot)
    list_troncon_suspect=df_troncon_noeud.loc[(df_troncon_noeud['rgraph_dbl']==0) & (df_troncon_noeud['rgraph_dbl_2']==1)].idtronc.tolist()
    if list_troncon_suspect :
        for t in  list_troncon_suspect : 
            troncon=Troncon(df_rdpt_simple,t)
            corresp_noeud_uniq=troncon.groupe_noeud_route_2_chaussees(graph,100)
            noeud_parrallele=corresp_noeud_uniq.loc[corresp_noeud_uniq['id_left']==num_noeud].id_right.values[0]
            #trouver les troncon qui intersectent ce troncon sur le debut ou la fin et qui n'ont pas été fléchés au début
            df_troncons_sup=df_tot.loc[((df_tot['source']==noeud_parrallele) | (df_tot['target']==noeud_parrallele)) & 
                                   (~df_tot.idtronc.isin(df_troncon_noeud.idtronc.tolist()))].idtronc.tolist()
            df_troncon_noeud_sup=df_noeud_troncon(df_rdpt_simple, noeud_parrallele,df_noeuds)
            df_troncon_noeud_sup=df_troncon_noeud_sup.loc[df_troncon_noeud_sup.idtronc.isin(df_troncons_sup)].copy()
            verif_double_sens(df_troncon_noeud_sup,df_tot)
            df_tronc_finale=pd.concat([df_troncon_noeud,df_troncon_noeud_sup], axis=0, sort=False).drop_duplicates('idtronc')
        return df_tronc_finale
    else : 
        return df_troncon_noeud

def type_estim(df_troncon_noeud):
    """
    deduire du nb de ligne et de valuer inconnu le type d'estimation a realiser
    in : 
        df_troncon_noeud : df issue de carac_troncon_noeud()
    out : 
        type_estim : string : 'calcul_3_voies' ou 'MMM'
    """
    if (len(df_troncon_noeud.idtronc.unique()) == 3 and 
        len(df_troncon_noeud.loc[df_troncon_noeud['tmjo_2_sens']==-99].idtronc.unique())==1) : 
        return 'calcul_3_voies'
    else : return 'MMM'

def separer_troncon_a_estimer(df):
    """
    isoler les troncon de trafic inconnu sur la base du tmjo_2_sens
    in : 
        df : dataframe des voies arrivants à un noeud, issu de df_noeud_troncon()
    """
    return df.loc[df['tmjo_2_sens']!=-99], df.loc[df['tmjo_2_sens']==-99]
    
def calcul_trafic_manquant_3troncons(df) : 
    """
    detreminer du trafic sur unnoeud a 3 voies avec 2 trafic connu
    in : 
        df : df des troncons liées au noeud
    """
    df_calcul_trafic_exist,df_calcul_trafic_null=separer_troncon_a_estimer(df)
    
    #calcul selon les cas de sens unique ou non
    if (df_calcul_trafic_exist.rgraph_dbl_2==0).all() : 
        if len(df_calcul_trafic_exist.type_noeud.unique())==1 : 
            return df_calcul_trafic_exist.tmjo_2_sens.sum()
        else : 
            if (df_calcul_trafic_null.type_noeud=='a').all() :
                return (df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='d'].tmjo_2_sens.values[0] - 
                        df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='a'].tmjo_2_sens.values[0])
            else :
                return (df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='a'].tmjo_2_sens.values[0] - 
                 df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='d'].tmjo_2_sens.values[0])
    elif (df_calcul_trafic_exist.rgraph_dbl_2!=0).all() :
        return df_calcul_trafic_exist.tmjo_2_sens.max()-df_calcul_trafic_exist.tmjo_2_sens.min()
    else : 
        if (df_calcul_trafic_null.rgraph_dbl_2==1).all() : 
            return df_calcul_trafic_exist.loc[df_calcul_trafic_exist['rgraph_dbl_2']==1].tmjo_2_sens.values[0]

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
    
    
class PasDeTraficError(Exception):
    """
    Exception levée si la ligne n'est pas affectée à du trafic
    """
    def __init__(self, idtroncon):
        Exception.__init__(self,f'pas de trafic sur le troncon : {idtroncon}')
        self.erreur_type='PasDeTraficError'  





