# -*- coding: utf-8 -*-
'''
Created on 16 dec. 2019

@author: martin.schoreisz

Module d'estimation des trafics à partir des trafic des points de comptages linearises
'''

import pandas as pd
import numpy as np
from Outils import plus_proche_voisin,nb_noeud_unique_troncon_continu,verif_index
from datetime import datetime
from collections import Counter


#quelques troncons dont les valeurs sont soit connues soit à imposer à -99 après calcul
#utilisé dans la fonction calculer_trafic_3voies
dico_corection_calcul_3_voies=[[25, 6000, 'mano'],[27, 500, 'mano'],[133, 5000, 'mano'],[155, 5000, 'mano'],[183, 5000, 'mano'],[186, 5000, 'mano'],[190, 15000, 'mano'],[191, 5000, 'mano'],[423, 6000, 'mano'],[425, 9000, 'mano'],
[450, 120000, 'mano'],[503, 500, 'mano'],[505, 110000, 'mano'],[524, 100000, 'mano'],[529, 300, 'mano'],[530, 11000, 'mano'],[542, 110000, 'mano'],[545, 10000, 'mano'],[553, 5000, 'mano'],
[582, -99],[606, 200, 'mano'],[653, 1000, 'mano'],[709, 100000, 'mano'],[849, 5000, 'mano'],[939, -99],[1015, 2000, 'mano'],[1041, 6000, 'mano'],[1124, 1000, 'mano'],[1192, -99],
[1206, 1000, 'mano'],[1236, 8000, 'mano'],[1254, 300, 'mano'],[1281, 7000, 'mano'],[1308, 1000, 'mano'],[1468, 2000, 'mano'],[1484, 3000, 'mano'],[1524, 1000, 'mano'],[1548, 9000, 'mano'],
[1601, -99],[1679, -99],[1926, 3000, 'mano'],[1939, 45000, 'mano'],[2112, -99],[2136, 16000, 'mano'],[2168, 1000, 'mano'],[2179, -99],[2240, 2000, 'mano'],[2253, 800, 'mano'],
[2271, 2000, 'mano'],[2280, 3000, 'mano'],[2477, 1500, 'mano'],[2560, 10000, 'mano'],[2577, 1000, 'mano'],[2658, 25000, 'mano'],[2713, 20000, 'mano'],[2789, 20000, 'mano'],[3126, 20000, 'mano'],
[3163, 45000, 'mano'],[3224, 3000, 'mano'],[3254, 12000, 'mano'],[3311, 10000, 'mano'],[3379, 5000, 'mano'],[3510, 5000, 'mano'],[3740, 6000, 'mano'],[3902, 5000, 'mano'],
[3913, 3000, 'mano'],[4081, 10000, 'mano'],[4128, 500, 'mano'],[4256, 1000, 'mano'],[4525, 50000, 'mano'],[4559, 500, 'mano'],[4699, 10000, 'mano'],[4831, 6000, 'mano'],[5101, 6000, 'mano'],
[5177, 1000, 'mano'],[5486, 10000, 'mano'],[6008, 300, 'mano'],[6322, 100, 'mano'],[6580, 6000, 'mano'],[6661, 4000, 'mano'],[6971, 40000, 'mano'],[7209, 2000, 'mano'],
[7323, 6000, 'mano'],[7392, 1000, 'mano'],[7503, 1942, 'mano'],[7889, 16000, 'mano'],[8368, 14000, 'mano'],[9719, 1000, 'mano'],[9899, 300, 'mano'],[10098, 16000, 'mano'],
[10667, 17000, 'mano'],[12864, 8000, 'mano'],[13210, 3000, 'mano'],[13219, 20000, 'mano'],[13241, -99, 'mano'],[2792333, 1000, 'mano'],
[2792334, 30000, 'mano'],[2792338, 1000, 'mano'],[2792339, 7000, 'mano'],[97,1500,'mano'],[4492,5500, 'mano'],[2409,500,'mano'],[1539,7929,'mano'], [507,5000,'mano']]
df_corection_calcul_3_voies=pd.DataFrame.from_records(dico_corection_calcul_3_voies, columns=['idtronc','tmjo_2_sens', 'type_cpt']).set_index('idtronc').fillna('NC')
liste_noeud_prioritaire=[6872,9754, 5634,5545,4855, 1613]


"""####################################################################
PARTIE COMMUNE 
###################################################################"""

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
    noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['tmjo_2_sens'])>2 and x['nb_nan'][-99]<len(x['tmjo_2_sens']) and x['nb_nan'][-99]!=0 else False,axis=1)
    noeud_estimable=noeud_grp.loc[noeud_grp['estimable']].copy()
    liste_noeud_estim=liste_noeud_prioritaire+[a for a in noeud_estimable.noeud.tolist() if a not in liste_noeud_prioritaire]
    return noeud_estimable,noeud_grp, df_noeuds,liste_noeud_estim

def df_noeud_troncon(df_tot, noeud,df_noeud_tot):
    """
    determiner la dataframe des troncon en rapport avec un noeud, en se basant surla ligne du troncon concerne
    in : 
        df_tot : ataframe du filaire de voie avec rond point simplifie et idtroncon. issu de simplifier_noeud_rdpt
        df_noeud_tot : dataframe de l'ensemnle des noeuds, issus de noeud_fv_ligne_ss_trafic()
    out : 
        df_noeud : dataframe des tronc arrivant sur le noeud avec id_ign,noeud, tmjo_2_sens,cat_rhv,type_noeud,idtronc
    """
    df_temp=df_tot.loc[(df_tot['source']==noeud) | (df_tot['target']==noeud)].copy()
    df_temp['type_noeud']=df_temp.apply(lambda x : 'd' if x['source']==noeud else 'a', axis=1 )
    df_noeud=df_noeud_tot.loc[df_noeud_tot['noeud']==noeud].merge(df_temp[['ident','id_ign','type_noeud', 'idtronc', 'type_cpt']], on='id_ign')
    return df_noeud.drop_duplicates()


def trouver_noeud_parrallele(Troncon,noeud,distance):
    """
    pour les troncon a 2*2 voies, trouver s'il existe un autre noeud assimilibale au 1er
    in : 
        Troncon : objet issu de la classe Troncon
        graoh : table des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
        noeud : integer : numero du noeud du carrefour
        distance : integer : distance max de recherche du noeud parrallele
    out : 
        noeud_parrallele : integer : numero du noeud ou None
    """
    if not Troncon.noeuds_uniques : #ca veut dire qu'une 2 fois 2 voies se termine de par et d'autre par un rd pt
        return None 
    corresp_noeud_uniq=Troncon.groupe_noeud_route_2_chaussees(distance)
    try :
        noeud_parrallele=corresp_noeud_uniq.loc[corresp_noeud_uniq['id_left']==noeud].id_right.values[0]
        return noeud_parrallele
    except IndexError : 
        return None
    

def carac_troncon_noeud(df_rdpt_simple, df_tot, graph,num_noeud,df_noeuds, lgn_rdpt):
    """
    Caractériser les troncons qui arrivent sur un noeud
    in : 
        df_rdpt_simple : ataframe du filaire de voie avec rond point simplifie et idtroncon
        df_tot : df du filaire sans modif des ronds points
        graoh : table des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
        num_noeud : integer : numero du noeud du carrefour
        df_noeuds : df de tout les oeuds, issu de noeuds_estimables()
        lgn_rdpt : df des lignes constituant les rd points, cf module Rond Points, du projet otv, dossier Base Bdtopo
    out : 
        df_tronc_finale : df des tronncon arrivant sur un noeud, toutes voies confodues (prise en compte 2*2 voies)
    """
    df_troncon_noeud=df_noeud_troncon(df_rdpt_simple, num_noeud,df_noeuds)
    df_troncon_noeud['rgraph_dbl_2']=df_troncon_noeud.apply(lambda x : 1 if Troncon(df_rdpt_simple,df_tot,x['idtronc'],lgn_rdpt,graph).double_sens else 0, axis=1)
    list_troncon_suspect=df_troncon_noeud.loc[(df_troncon_noeud['rgraph_dbl']==0) & (df_troncon_noeud['rgraph_dbl_2']==1)].idtronc.tolist()
    if list_troncon_suspect :
        for t in  list_troncon_suspect : 
            noeud_parrallele=trouver_noeud_parrallele(Troncon(df_rdpt_simple,df_tot,t,lgn_rdpt,graph), num_noeud,30)
            if not noeud_parrallele or noeud_parrallele not in df_noeuds.noeud.tolist() :
                continue
            #trouver les troncon qui intersectent ce troncon sur le debut ou la fin et qui n'ont pas été fléchés au début
            df_troncons_sup=df_tot.loc[((df_tot['source']==noeud_parrallele) | (df_tot['target']==noeud_parrallele)) & 
                                   (~df_tot.idtronc.isin(df_troncon_noeud.idtronc.tolist()))].idtronc.tolist()
            df_troncon_noeud_sup=df_noeud_troncon(df_rdpt_simple, noeud_parrallele,df_noeuds)
            df_troncon_noeud_sup=df_troncon_noeud_sup.loc[df_troncon_noeud_sup.idtronc.isin(df_troncons_sup)].copy()
            if df_troncon_noeud_sup.empty : 
                continue
            df_troncon_noeud_sup['rgraph_dbl_2']=df_troncon_noeud_sup.apply(lambda x : 1 if Troncon(df_rdpt_simple,df_tot,x['idtronc'],lgn_rdpt,graph).double_sens else 0, axis=1)
            df_tronc_finale=pd.concat([df_troncon_noeud,df_troncon_noeud_sup], axis=0, sort=False).drop_duplicates('idtronc')
        try :
            return df_tronc_finale.reset_index().drop('index',axis=1) #pour avoir un index unique
        except UnboundLocalError : 
            return df_troncon_noeud.drop_duplicates('idtronc').reset_index().drop('index',axis=1)
    else : 
        return df_troncon_noeud.drop_duplicates('idtronc').reset_index().drop('index',axis=1)

def creer_dico_troncons_noeud(df_troncon_noeud,gdf_rhv_rdpt_simple,gdf_base,lgn_rdpt,graph_filaire_123_vertex ):
    """
    creer un dico avec en clé l'ditronc et en value les objets Troncon correspondants
    in : 
        df_troncon_noeud : df issue de carac_troncon_noeud()
        gdf_rhv_rdpt_simple : ataframe du filaire de voie avec rond point simplifie et idtroncon
        gdf_base : df du filaire sans modif des ronds points
        graph_filaire_123_vertex : table des vertex du graph sans modif liées au rond points
        num_noeud : integer : numero du noeud du carrefour
        lgn_rdpt : df des lignes constituant les rd points, cf module Rond Points, du projet otv, dossier Base Bdtopo
    """
    return {a:Troncon(gdf_rhv_rdpt_simple,gdf_base,a,lgn_rdpt,graph_filaire_123_vertex) for a in  df_troncon_noeud.idtronc.tolist()}


"""####################################################################
PARTIE CALCUL 3 VOIES
###################################################################"""

def type_estim(df_troncon_noeud):
    """
    deduire du nb de ligne et de valuer inconnu le type d'estimation a realiser
    in : 
        df_troncon_noeud : df issue de carac_troncon_noeud()
    out : 
        type_estim : string : 'calcul_3_voies' ou 'MMM'
    """
    df_calcul_trafic_exist,df_calcul_trafic_null=separer_troncon_a_estimer(df_troncon_noeud)
    if ((len(df_troncon_noeud.idtronc.unique()) == 3 and # cas simple en ville
        len(df_troncon_noeud.loc[df_troncon_noeud['tmjo_2_sens']==-99].idtronc.unique())==1) or 
         (len(df_calcul_trafic_null)== 1  and  len(df_calcul_trafic_exist)%2==0 and #cas des bretelles d'autoroute
        len(df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='a'])%2==0 and 
         len(df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='d'])%2==0 ) ): 
        return 'calcul_3_voies'
    else : return 'MMM'

def separer_troncon_a_estimer(df):
    """
    isoler les troncon de trafic inconnu sur la base du tmjo_2_sens
    in : 
        df : dataframe des voies arrivants à un noeud, issu de df_noeud_troncon()
    """
    return df.loc[df['tmjo_2_sens']!=-99], df.loc[df['tmjo_2_sens']==-99]
    

def maj_trafic_3tronc(df):
    """
    mettre a jour les trafics inconnues sur un noeud à 3 troncons entrant
    in : 
        df : df des troncons liées au noeud
    """
    
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
                trafic=df_calcul_trafic_exist.tmjo_2_sens.sum()
            else : 
                if (df_calcul_trafic_null.rgraph_dbl_2==0).all() :
                    if (df_calcul_trafic_null.type_noeud=='a').all() :
                        trafic=(df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='d'].tmjo_2_sens.values[0] - 
                                df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='a'].tmjo_2_sens.values[0])
                    else :
                        trafic=(df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='a'].tmjo_2_sens.values[0] - 
                         df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='d'].tmjo_2_sens.values[0])
                else : #─ dans ce cas les lignes de trafic existante sont forcementg une arrivee et un depart, sinon celui que l'on cherche ne serait pas double sens
                    if len(df_calcul_trafic_exist)==2 : #cas de voies simple en agglo
                        trafic=trafic=df_calcul_trafic_exist.tmjo_2_sens.sum()
                    elif (len(df_calcul_trafic_exist)%2==0 and #cas des bretelles d'autoroute
                          len(df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='a'])%2==0 and 
                          len(df_calcul_trafic_exist.loc[df_calcul_trafic_exist['type_noeud']=='d'])%2==0):
                        trafic=df_calcul_trafic_exist.tmjo_2_sens.sum()
                    else : 
                        trafic=-999999999
        elif (df_calcul_trafic_exist.rgraph_dbl_2!=0).all() :
            trafic=df_calcul_trafic_exist.tmjo_2_sens.max()-df_calcul_trafic_exist.tmjo_2_sens.min()
        else : 
            if (df_calcul_trafic_null.rgraph_dbl_2==1).all() : 
                trafic=df_calcul_trafic_exist.loc[df_calcul_trafic_exist['rgraph_dbl_2']==1].tmjo_2_sens.values[0]
            else : 
                trafic=-999999999
        if trafic<50 and trafic !=-999999999 : 
            return -99, "estim_manuelle"
        elif trafic ==-999999999 : 
            return -99, "estim_a_travailer"
        else : return trafic, "estim_calcul_3_voies"
    
    if df.reset_index().duplicated('index').any() : #inon on peut avoir une errur si le numero d'index est dupliqué
        df=df.reset_index().drop('index',axis=1)
    df.loc[df['tmjo_2_sens']==-99,'type_cpt']=df.loc[df['tmjo_2_sens']==-99].apply(lambda x : calcul_trafic_manquant_3troncons(df)[1], axis=1)
    df.loc[df['tmjo_2_sens']==-99,'tmjo_2_sens']=df.loc[df['tmjo_2_sens']==-99].apply(lambda x : calcul_trafic_manquant_3troncons(df)[0], axis=1)

def maj_traf(df, df_traf):
    """
    mettre à jour une df selon le traf d'une autre, en jouant sur les index
    in : 
        df : df à mettre à jour, normalement c'est la df issu de l'affectation des pt de comptag ou celle issu de calculer_trafic_3voies
        df_traf : la df issu des calculs, contenant les attributs 'tmjo_2_sens' et 'type_cpt', avec 'idtronc' en index
    """
    df.set_index('idtronc',inplace=True)
    df.update(df_traf[['tmjo_2_sens','type_cpt']])
    df.reset_index(inplace=True)

def calculer_trafic_3voies(gdf_base,gdf_rhv_rdpt_simple,lgn_rdpt,graph_filaire_123_vertex):
    """
    pour les noeuds qui supportent 3 voies et dont on connait le trafic sur 2, on déduit le trafic sur la 3ème selon les sens de circulation.
    attention, on modifie sur place le prarametre d'entree gdf_base
    in :
       gdf_base : gdf des données avec les trafics issus de ptde comptage (cf module Affectation_pt_comptage
       gdf_rhv_rdpt_simple : gdf_base avec modif des source et target aux rd pt cf Simplifier_rd_pt.modifier_source_target()
       graph_filaire_123_vertex : table des vertex du graph sans modif liées au rond points
       lgn_rdpt : gdf des lignes faisant partie d'un rd pt, cf identifier_ronds_points() 
    """
    df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
    dico_noeud_3_voies, liste_noeud_traite={},[]
    i=0
    while liste_noeud_estim : 
        # pour chaque noeuds : 
        #calcul des troncons arrivants
        print(f'debut boucle {datetime.now()}')
        for j,num_noeud in enumerate(liste_noeud_estim) : 
            if j%400==0 : print(f'stocker valeur : {num_noeud} iter : {j}')
            df_troncon_noeud=carac_troncon_noeud(gdf_rhv_rdpt_simple, gdf_base, graph_filaire_123_vertex,num_noeud,df_noeuds,lgn_rdpt)
            #si tous les trafics du noeud sont déjà renseignés on passe
            if (df_troncon_noeud.tmjo_2_sens!=-99).all() : 
                continue
            #determiner si on est dans le cas d'une voie pouvant etre estimée par calcul (3 troncon dont 1 seul manquant) ou par analogie avec le MMM
            type_calcul=type_estim(df_troncon_noeud)
            if type_calcul=='calcul_3_voies' :
                dico_noeud_3_voies[num_noeud]=df_troncon_noeud
     
        print(f'nb noeud 3 voies {len(dico_noeud_3_voies)} ;  {datetime.now()}')
        if len(dico_noeud_3_voies)==0 : break
        
        print(f'calcul des  valeur, debut :  {datetime.now()}')
        for num_noeud, df_troncon_noeud in dico_noeud_3_voies.items() :
            gdf_rhv_rdpt_simple=verif_index(gdf_rhv_rdpt_simple,'idtronc')#Îsi idtronc est en index on le repasse dans les colonnes
            if (df_troncon_noeud.tmjo_2_sens!=-99).all() : 
                continue
            #calcul trafic
            maj_trafic_3tronc(df_troncon_noeud)
            df_update_traf=df_troncon_noeud[['idtronc','tmjo_2_sens','type_cpt']].set_index('idtronc').drop_duplicates()
            df_update_traf.update(df_corection_calcul_3_voies)
            #mise à jour du fichier source
            maj_traf(gdf_rhv_rdpt_simple, df_update_traf)
            maj_traf(gdf_base, df_update_traf)
            liste_noeud_traite.append(num_noeud)
        df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
        liste_noeud_estim=[n for n in liste_noeud_estim if n not in liste_noeud_traite]
        dico_noeud_3_voies={}
        i+=1
        print(f'fin repetition num : {i}')

"""####################################################################
PARTIE CALCUL MMM
###################################################################"""

def matrice_troncon_noeud_rhv(df_troncon_noeud,num_noeud,lgn_rdpt,dico_troncons_noeud) :
    """
    df des lignes arrivant sur un noeud, avec la ligne de départ en face
    in : 
        dico_troncons_noeud : dico des objets Troncons, cf creer_dico_troncons_noeud()
        num_noeud : integer : numero du noeud du carrefour
        lgn_rdpt : df des lignes constituant les rd points, cf module Rond Points, du projet otv, dossier Base Bdtopo
    out : 
        tab_corresp_rhv : la df issue du cross join des lignes de df_troncon_noeud, filtree
    """
    #filtrer les lignes des rd pt
    if any([a in lgn_rdpt.id_ign.tolist() for a in df_troncon_noeud.id_ign.tolist()]) : 
        idtronc=df_troncon_noeud.loc[[a in lgn_rdpt.id_ign.tolist() for a in df_troncon_noeud.id_ign.tolist()]].idtronc.to_numpy()[0]
        ident=dico_troncons_noeud[idtronc].lgn_ss_rdpt.loc[(dico_troncons_noeud[idtronc].lgn_ss_rdpt['source']==num_noeud) | 
                                                           (dico_troncons_noeud[idtronc].lgn_ss_rdpt['target']==num_noeud)].ident.to_numpy()[0]
        df_troncon_noeud.loc[df_troncon_noeud['idtronc']==idtronc,'ident']=ident
        df_troncon_noeud.loc[df_troncon_noeud['idtronc']==idtronc,'id_ign']='TRONROUT'+str(ident)
        
    cross_join=df_troncon_noeud.assign(key=1).merge(df_troncon_noeud.assign(key=1),on="key").drop("key", axis=1)
    cross_join['filtre']=cross_join.apply(lambda x : tuple(sorted((x['ident_x'],x['ident_y']))),axis=1)
    cross_join.drop_duplicates('filtre', inplace=True)
    tab_corresp_rhv=cross_join.loc[cross_join['ident_x']!=cross_join['ident_y']].drop('filtre',axis=1).copy()[['ident_x',
                'ident_y','idtronc_x','idtronc_y','tmjo_2_sens_x','tmjo_2_sens_y','cat_rhv_x','cat_rhv_y','type_cpt_x','type_cpt_y']]
    return tab_corresp_rhv

def estim_mmm_jointure_voies(matrice_voie_rhv,cle_mmm_rhv):
    """
    depuis les voies rhv d'un noeud, trouver les voies mmm correspondantes
    in : 
        matrice_voie_rhv : issu de corresp_noeud_rhv
        cle_mmm_rhv : issu du travail postgis
        
    out : 
        cle_mmm_rhv
    """

    joint_fv_mmm_e1=matrice_voie_rhv.merge(cle_mmm_rhv[['NO','ident']], left_on='ident_x', right_on='ident', how='left').drop_duplicates().rename(
        columns={'NO':'NO_x'}).drop('ident', axis=1)
    if joint_fv_mmm_e1.NO_x.isna().any() : 
        raise PasCorrespondanceError(list(set(joint_fv_mmm_e1.loc[joint_fv_mmm_e1.NO_x.isna()].ident_x.tolist())))
    joint_fv_mmm_e2=joint_fv_mmm_e1.merge(cle_mmm_rhv[['NO','ident']], left_on='ident_y', right_on='ident',how='left').drop_duplicates().rename(
            columns={'NO':'NO_y'}).drop('ident', axis=1)
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
    trafic_inconnus=joint_fv_mmm_e2.loc[((joint_fv_mmm_e2['tmjo_2_sens_x']==-99) & (joint_fv_mmm_e2['tmjo_2_sens_y']!=-99)) | 
                                     ((joint_fv_mmm_e2['tmjo_2_sens_x']!=-99) & (joint_fv_mmm_e2['tmjo_2_sens_y']==-99))].copy()
    if  trafic_inconnus.empty : 
        raise PasDeTraficInconnuError()
    trafic_inconnus['idtronc_a_estim']=trafic_inconnus.apply(lambda x : x['idtronc_x'] if x['tmjo_2_sens_x']==-99
                                                      else x['idtronc_y'], axis=1)
    #on ne garde que les ligne de catégorie la plus proche
    trafic_inconnus['diff_cat']=trafic_inconnus.apply(lambda x : abs((int(x['cat_rhv_x'])-int(x['cat_rhv_y']))), axis=1)
    trafic_inconnus_prior_cat=trafic_inconnus.loc[trafic_inconnus['diff_cat']==trafic_inconnus.groupby(['ident_x','ident_y']).diff_cat.transform(min)]
    trafic_inconnus_prior_cat=trafic_inconnus_prior_cat.loc[trafic_inconnus_prior_cat['diff_cat']==trafic_inconnus_prior_cat.
                                                            groupby('idtronc_a_estim').diff_cat.transform(min)]
    return trafic_inconnus_prior_cat

def trafic_mmm(gdf_calcul,trafic_inconnus_prior_cat,mmm_simple,cle_mmm_rhv, df_troncon_noeud,dico_troncons_noeud):
    """
    associer les trafc MMM aux voies rhv d'un noeud
    in : 
        trafic_inconnus_prior_cat : cf isoler_trafic_inconnu
        mmm_simple : df des voies mmm avec une valeur de tmja
        noeud : integer : noeud du filaire de voie concerne
        graoh : table des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
        cle_mmm_rhv : issu du travail postgis
        df_rdpt_simple : ataframe du filaire de voie avec rond point simplifie et idtroncon
    """
    def trafic_reference_mmm(tmjo_2_sens_x,tmjo_2_sens_y, df, idtronc_x,idtronc_y) : 
        """
        trouver le tmjo de reference du mmm pour le calcul des trafic manquants,si plueisuers lignes possible
        in : 
           tmjo_2_sens_x : float : trafic issu du filaire prealablement renseigne
           tmjo_2_sens_y : float : trafic issu du filaire prealablement renseigne
           df : df des voies a renseignees
           idtronc : float : ifentifiant du troncon concerne
        """
        if tmjo_2_sens_y == -99 : 
            df_traf_ref=df.loc[(df['idtronc_y']==idtronc_y) | (df['idtronc_x']==idtronc_y) ].copy()
            df_traf_ref['traf_ref']=df_traf_ref.apply(lambda x : x['tmja_tv_x'] if x['tmjo_2_sens_y']==-99 else x['tmja_tv_y'],axis=1)
            return df_traf_ref.traf_ref.max()
        else : 
            df_traf_ref=df.loc[(df['idtronc_y']==idtronc_x) | (df['idtronc_x']==idtronc_x) ].copy()
            df_traf_ref['traf_ref']=df_traf_ref.apply(lambda x : x['tmja_tv_x'] if x['tmjo_2_sens_y']==-99 else x['tmja_tv_y'],axis=1)
            return df_traf_ref.traf_ref.max()
      
    def somme_traf(idtronc, traf_rens) : 
        """
        pour les voies du MMM représenté par une ligne qui se sépare en 2 dur la fin (rd point par exemple))
        """
        nb_value=list(set(traf_rens.loc[traf_rens['idtronc_x']==idtronc].tmja_tv_x.tolist()+traf_rens.loc[traf_rens['idtronc_y']==idtronc].tmja_tv_y.tolist()))
        if len(nb_value)>=2 : 
            return (sum(nb_value))
        else : return (nb_value[0])
    
    #test si les troncon MMM identifiés ont bien un trafic dans le fichier mmm simplifie. si pas le cas corrige le fichier simplifie
    troncon_mmm_sstraf=[a for a  in trafic_inconnus_prior_cat.NO_x.tolist()+trafic_inconnus_prior_cat.NO_y.tolist() if a not in mmm_simple.NO.tolist()]
    if troncon_mmm_sstraf : 
        raise PasDeTraficError(troncon_mmm_sstraf)
    traf_mmm=trafic_inconnus_prior_cat.merge(mmm_simple[['NO','tmja_tv']], left_on='NO_x', right_on='NO').drop('NO', axis=1).merge(
    mmm_simple[['NO','tmja_tv']], left_on='NO_y', right_on='NO').drop('NO', axis=1)
    
    #mise a jour des trafic mmm si 2*2voies MMM :
    df_tronc_suspect=df_troncon_noeud.loc[(df_troncon_noeud['rgraph_dbl']==0) & (df_troncon_noeud['rgraph_dbl_2']==1)]
    if df_tronc_suspect.idtronc.tolist() : 
        for i,ts in enumerate(df_tronc_suspect.idtronc.tolist()) :
            #le noeud parrallèle
            noeud_par=trouver_noeud_parrallele(dico_troncons_noeud[ts],df_tronc_suspect.iloc[i].noeud,40)
            if not noeud_par : #peut etre le cas d'une 2*2 terminéee par un rd pt, ou d'une simple voie se separanten 2 à la fin
                if dico_troncons_noeud[ts].rd_pt_flag : #☻cas du rd point
                    df_noeud_par=dico_troncons_noeud[ts].groupe_noeud_route_2_chaussees_rd_pt(40)
                    try :
                        lgn_sup=gdf_calcul.loc[((gdf_calcul.source.isin(df_noeud_par.id_left.tolist())) | (gdf_calcul.source.isin(df_noeud_par.id_left.tolist()))) & 
                              (~gdf_calcul.ident.isin(df_troncon_noeud.ident.tolist())) & 
                              (~gdf_calcul.ident.isin(dico_troncons_noeud[ts].df_lgn_rdpt.ident.tolist()))].ident.to_numpy()[0]
                    except IndexError : continue
                else : #cas d'un epatte d'oie, pas traité pour le moment
                    continue
            #lignes rhv qui touchent ce noeud et uqui ont le mm idtrronc
            else : 
                lgn_sup=gdf_calcul.loc[(gdf_calcul['idtronc']==ts) & ((gdf_calcul['source']==noeud_par) | (gdf_calcul['target']==noeud_par))].ident.to_numpy()[0]
            #equivalent mmm
            try :
                lgn_sup_mmm=cle_mmm_rhv.loc[cle_mmm_rhv['ident']==lgn_sup].NO.to_numpy()[0]
            except IndexError : continue #ça veut dire que 2 lignes RHV sont représentées par 1 seule MMM, dc pas besoin de sommer
            #test si ligne et idtronc deja presents dans traficrens
            if not pd.isnull(lgn_sup_mmm) : #·si lgn_sup_mmm est NaN, ça veut juste dire qu'il n'y a qu'une seule ligne MMM pour 2 lignes RHV donc pas de pb
                if not any(((traf_mmm['NO_x']==lgn_sup_mmm) & (traf_mmm['idtronc_x']==ts)) | ((traf_mmm['NO_y']==lgn_sup_mmm) & (traf_mmm['idtronc_y']==ts))) :
                    try : 
                        traf_ajout=mmm_simple.loc[mmm_simple['NO']==lgn_sup_mmm].tmja_tv.to_numpy()[0]
                    except IndexError : continue
                    traf_mmm['tmja_tv_x']=traf_mmm.apply(lambda x : x['tmja_tv_x']+traf_ajout if x['idtronc_x']==ts else x['tmja_tv_x'], axis=1)
                    traf_mmm['tmja_tv_y']=traf_mmm.apply(lambda x : x['tmja_tv_y']+traf_ajout if x['idtronc_y']==ts else x['tmja_tv_y'], axis=1) 
            
    #ensuite on refait un test dans le cas par exemple des rd pt pour lesquels les voies se séparent à la fin
    traf_mmm2=traf_mmm.copy()
    traf_mmm2['tmja_tv_x']=traf_mmm2.apply(lambda x : somme_traf(x['idtronc_x'], traf_mmm),axis=1)
    traf_mmm2['tmja_tv_y']=traf_mmm2.apply(lambda x : somme_traf(x['idtronc_y'], traf_mmm),axis=1)
    
    traf_mmm2['traf_max']=traf_mmm2.apply(lambda x:trafic_reference_mmm(x['tmjo_2_sens_x'],x['tmjo_2_sens_y'],traf_mmm2,x['idtronc_x'],x['idtronc_y']), axis=1)
    traf_mmm2.drop_duplicates(['idtronc_x','idtronc_y','tmja_tv_x','tmja_tv_y','traf_max'], inplace=True)
    return traf_mmm2

def calcul_trafic_rhv_depuisMMM(traf_mmm):
    """
    sur un noeud, calculer les trafic RHV à partir des trafics MMM et d'au moins un trafic RHV connu
    in : 
        traf_mmm : cf trafic_mmm
    out :
        trafc_fin : df des lignes du noeud avec la valeur de trafic et l'idtronc correspodant     
    """
    trafc_rens=traf_mmm.copy()
    trafc_rens['idtronc']=trafc_rens.apply(lambda x : x['idtronc_x'] if x['tmjo_2_sens_x']==-99
                                                      else x['idtronc_y'], axis=1)
    trafc_rens['ident_ref_calcul']=trafc_rens.apply(lambda x : 'x' if x['tmjo_2_sens_y']==-99
                                                          else 'y', axis=1)
    #si plusieurs resultas possibles pour unident on garde le max
    trafc_rens=trafc_rens.loc[trafc_rens.apply(lambda x : x['traf_max']==x['tmja_tv_'+x['ident_ref_calcul']], axis=1)].copy()
    
    try : 
        trafc_rens['tmjo_2_sens_extrapol']=trafc_rens.apply(lambda x : 
            int(x['tmjo_2_sens_x']/x['tmja_tv_x']*x['tmja_tv_y']) if x['tmjo_2_sens_y']==-99 else int(x['tmjo_2_sens_y']/x['tmja_tv_y']*x['tmja_tv_x']),
                                                          axis=1)
        #check si une seule valeur par idtronc que l'on cherche, si pas le cas on prend la valeur calculee max
        if not (trafc_rens.groupby('idtronc').tmjo_2_sens_extrapol.nunique()==1).all() : 
            trafc_rens=trafc_rens.loc[trafc_rens.tmjo_2_sens_extrapol==trafc_rens.groupby('idtronc').tmjo_2_sens_extrapol.transform(max)].copy()   
            
    except (ZeroDivisionError, ValueError) : 
        raise PasDeTraficError (trafc_rens.idtronc.tolist())
    trafc_rens=trafc_rens[['idtronc','tmjo_2_sens_extrapol']].drop_duplicates()
    return trafc_rens

def calculer_trafic_mmm(gdf_calcul, gdf_rhv_rdpt_simple,lgn_rdpt,graph_filaire_123_vertex,mmm_simple,cle_mmm_rhv):
    """pour les noeuds qui supportent 3 voies et dont on connait le trafic sur 2, on déduit le trafic sur la 3ème selon les sens de circulation.
    attention, on modifie sur place le prarametre d'entree gdf_base
    in :
       gdf_base : gdf des données avec les trafics issus de ptde comptage (cf module Affectation_pt_comptage
       gdf_rhv_rdpt_simple : gdf_base avec modif des source et target aux rd pt cf Simplifier_rd_pt.modifier_source_target()
       graph_filaire_123_vertex : table des vertex du graph sans modif liées au rond points
       lgn_rdpt : gdf des lignes faisant partie d'un rd pt, cf identifier_ronds_points()
       cle_mmm_rhv : gdf permettant le lien entre les lignes du rhv et les lignes du mmm, de MMM.import_fichiers_mmm
       mmm_simple : gdf du mmm avec rergouepemnt des attributs de trafic et simpmlification du nb d'attributs, issu de MMM.import_fichiers_mmm
       
    """
    #correction mano des points non présents dans le calcul 3 voies
    maj_traf(gdf_rhv_rdpt_simple, df_corection_calcul_3_voies.loc[df_corection_calcul_3_voies['tmjo_2_sens']!=-99])
    maj_traf(gdf_calcul, df_corection_calcul_3_voies.loc[df_corection_calcul_3_voies['tmjo_2_sens']!=-99])
    
    df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
    liste_noeud_traite=[]
    dico_erreur={}
    while liste_noeud_estim : 
        num_noeud=liste_noeud_estim[0]
        print(num_noeud)
        liste_noeud_traite.append(num_noeud)
        df_troncon_noeud=carac_troncon_noeud(gdf_rhv_rdpt_simple, gdf_calcul, graph_filaire_123_vertex,num_noeud,df_noeuds,lgn_rdpt)
        dico_troncons_noeud=creer_dico_troncons_noeud(df_troncon_noeud,gdf_rhv_rdpt_simple,gdf_calcul,lgn_rdpt,graph_filaire_123_vertex )
        matrice_rhv=matrice_troncon_noeud_rhv(df_troncon_noeud,num_noeud,lgn_rdpt,dico_troncons_noeud) 
        try :
            joint_fv_mmm_e2=estim_mmm_jointure_voies(matrice_rhv,cle_mmm_rhv)
        except PasCorrespondanceError : 
            dico_erreur[num_noeud]='PasCorrespondanceError'
            df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
            liste_noeud_estim=[n for n in liste_noeud_estim if n not in liste_noeud_traite]
            continue
        try :
            trafic_inconnus_prior_cat=isoler_trafic_inconnu(joint_fv_mmm_e2)
        except PasDeTraficInconnuError: 
            dico_erreur[num_noeud]='PasDeTraficInconnuError' 
            df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
            liste_noeud_estim=[n for n in liste_noeud_estim if n not in liste_noeud_traite]
            continue
        try :
            trafc_rens=trafic_mmm(gdf_calcul,trafic_inconnus_prior_cat,mmm_simple,cle_mmm_rhv, df_troncon_noeud,dico_troncons_noeud)
            df_update_traf=calcul_trafic_rhv_depuisMMM(trafc_rens).set_index('idtronc').rename(columns={'tmjo_2_sens_extrapol':'tmjo_2_sens'})
        except PasDeTraficError as e: 
            dico_erreur[num_noeud]=e 
            df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
            liste_noeud_estim=[n for n in liste_noeud_estim if n not in liste_noeud_traite]
            continue
        df_update_traf['type_cpt']='calcul_mmm'
        #try : 
        maj_traf(gdf_rhv_rdpt_simple, df_update_traf)
        maj_traf(gdf_calcul, df_update_traf)
    
        liste_noeud_traite.append(num_noeud)
        df_noeuds,liste_noeud_estim=noeuds_estimables(gdf_rhv_rdpt_simple)[2:4]
        liste_noeud_estim=[n for n in liste_noeud_estim if n not in liste_noeud_traite]

"""####################################################################
PARTIE CLASSES
###################################################################"""

class Troncon(object):
    """
    caractériser un troncon continu.
    attribut : 
        id : identifiant 
        df_lgn : dataframe des lignes qui constituent le troncon
        df_tot : df des lignes totales du jdd contenant le tronc,sans simplification des rdpts
        nb_lgn : nb de ligne du troncon,
        noeuds_uniques, noeuds : lists des noeuds de fin de troncon et des noeuds du troncon
        nb_noeuds_uniques, nb_noeuds : nombre de noeuds et de noeud de fin de traoncon
        df_lign_fin_tronc : dataframe des lignes de fin de troncon
        rd_pt_flag : booleen : traduit si le troncon comporte un rond point
        df_lgn_rdpt : df des lignes faisant partuie d'un rond point
        nb_lgn_ss_rdpt : nombre de lignes sans celles constituant un rd pt
        double_sens : booleen : traduit si troncon en double sens ou non
    """
    def __init__(self, df_rdpt_simple,df_tot, num_tronc, lgn_rdpt, graph_tot_vertex):
        """
        constrcuteur
        in : 
            df_rdpt_simple  : dataframe du filaire de voie avec rond point simplifie et idtroncon
            df_tot : dataframe du filaire de voie sans simplification des rond point
            graph_tot : vertex du graph associe à df_tot (typiquement la table xxxx_vertices_pgr dans pg_routing, avaec analyze graph
            num_tronc : numero du troncon
            lgn_rdpt : df des lignes constituant les rd points, cf module Rond Points, du projet otv, dossier Base Bdtopo
        """
        self.id=num_tronc
        self.df_tot=df_tot
        self.graph_tot_vertex=graph_tot_vertex
        self.rd_pt_flag, self.df_lgn_rdpt, self.nb_lgn_ss_rdpt, self.lgn_ss_rdpt, self.noeud_rd_pt=self.rond_points(lgn_rdpt)
        self.df_lgn=df_rdpt_simple.loc[df_rdpt_simple['idtronc']==num_tronc].copy()
        self.nb_lgn=len(self.df_lgn)
        self.noeuds_uniques_simplifie, self.noeuds_simplifie=nb_noeud_unique_troncon_continu(df_rdpt_simple,num_tronc,'idtronc')
        self.noeuds_uniques, self.noeuds=nb_noeud_unique_troncon_continu(df_tot,num_tronc,'idtronc')
        self.nb_noeuds_simplifie, self.nb_noeuds_uniques_simplifie=len(self.noeuds_simplifie), len(self.noeuds_uniques_simplifie)
        self.nb_noeuds, self.nb_noeuds_uniques=len(self.noeuds), len(self.noeuds_uniques)
        self.df_lign_fin_tronc_simplifie=df_rdpt_simple.loc[((df_rdpt_simple['source'].isin(self.noeuds_uniques_simplifie)) | (df_rdpt_simple['target'].isin(self.noeuds_uniques_simplifie))) & 
                       (df_rdpt_simple['idtronc']==num_tronc)]
        self.nb_lgn_tch_noeud_unique_simplifie=len(self.df_lign_fin_tronc_simplifie)
        
        self.double_sens=self.double_sens()
    
    def troncon_touche(self,df_rdpt_simple):
        """
        trouver les troncons qui touchent, avec la valeur du noeud d'intersection
        in : 
            df_rdpt_simple : dataframe du filaire de voie avec rond point simplifie et idtroncon
        out : 
            df_tronc_tch : dataframe avec idtronc et noeud
        """
        df_tronc_tch=df_rdpt_simple.loc[((df_rdpt_simple['source'].isin(self.noeuds_simplifie_uniques_simplifie)) | (df_rdpt_simple['target'].isin(self.noeuds_simplifie_uniques_simplifie))) & 
                        (df_rdpt_simple['idtronc']!=self.id)]
        df_tronc_tch=pd.concat([df_tronc_tch[['idtronc','source']].rename(columns={'source':'noeud'}),
                                df_tronc_tch[['idtronc','target']].rename(columns={'target':'noeud'})], axis=0, sort=False)
        df_tronc_tch=df_tronc_tch.loc[df_tronc_tch['noeud'].isin(self.noeuds_simplifie_uniques_simplifie)].copy()
        return df_tronc_tch
    
    def groupe_noeud_route_2_chaussees (self, distance):
        """
        pour un troncon constitue d'une voie a 2 chaussee, obtenirune correspondance entre les 4 noeuds uniques, en les groupant 2 par 2
        in : 
            graph : df des noeuds
            distance : distance max entre les noeuds a grouper
        out : 
            corresp_noeud_uniq : df avec les id a associer
        """
        df_noeud_uniq=self.graph_tot_vertex.loc[(self.graph_tot_vertex['id'].isin(self.noeuds_uniques))]
        corresp_noeud_uniq=plus_proche_voisin(df_noeud_uniq, df_noeud_uniq, distance, 'id', 'id', True)
        return corresp_noeud_uniq
    
    def rond_points(self,lgn_rdpt):
        """
        savoir si le troncon contient un rond point ou non
        in :
            lgn_rdpt : df des lignes constituant les rd points, cf module Rond Points, du projet otv, dossier Base Bdtopo
        out : 
            rd_pt_flag : booleen : traduit si un rdpoint est dans le troncon
            df_rdpt : df des lignes constituant le rdpt
            nb_lgn_ss_rdpt : nombre de ligne sans celles qui constituent le rdpoints
        """
        noeud_rd_pt_tt_ligne=set(lgn_rdpt.source.tolist()+lgn_rdpt.target.tolist())
        df_lgn_rdpt=self.df_tot.loc[(self.df_tot.ident.isin(lgn_rdpt.ident.tolist())) & (self.df_tot.idtronc==self.id)]
        lgn_ss_rdpt=self.df_tot.loc[(~self.df_tot.ident.isin(lgn_rdpt.ident.tolist())) & (self.df_tot.idtronc==self.id)]
        noeud_rd_pt=set(self.df_tot.loc[(self.df_tot.source.isin(noeud_rd_pt_tt_ligne)) & (self.df_tot.idtronc==self.id) ].source.tolist()
                     +self.df_tot.loc[(self.df_tot.target.isin(noeud_rd_pt_tt_ligne)) & (self.df_tot.idtronc==self.id) ].target.tolist())
        if not df_lgn_rdpt.empty : 
            return True, df_lgn_rdpt, len(lgn_ss_rdpt), lgn_ss_rdpt, list(noeud_rd_pt)
        else : return False, pd.DataFrame(),self.nb_lgn, self.df_lgn, []
    
    def groupe_noeud_route_2_chaussees_rd_pt(self,distance):
        """
        pour une 2*2 voies arriavnt sur un rd pt, si on veut connaitre les noeud de la 2*2 intersectant le rd pt
        """
        noeud_uniq=nb_noeud_unique_troncon_continu(self.df_tot.loc[self.df_tot.ident.isin(self.lgn_ss_rdpt.ident.tolist())], self.id, 'idtronc')[0]
        #print(noeud_uniq)
        df_noeud_uniq=self.graph_tot_vertex.loc[(self.graph_tot_vertex['id'].isin(noeud_uniq))]
        corresp_noeud_uniq=plus_proche_voisin(df_noeud_uniq, df_noeud_uniq, distance, 'id', 'id', True)
        df_lgn_rdpt=self.df_tot.loc[self.df_tot.ident.isin(self.df_lgn_rdpt.ident.tolist())]
        corresp_noeud_uniq=corresp_noeud_uniq.loc[corresp_noeud_uniq.id_left.isin(df_lgn_rdpt.source.tolist()+df_lgn_rdpt.target.tolist())].copy()
        return corresp_noeud_uniq
    
    def double_sens(self):
        """
        savoir si le troncon est double sens ou non
        """
        if not self.noeuds_uniques_simplifie : 
            return False
        if (self.df_lign_fin_tronc_simplifie.rgraph_dbl==0).all() : 
            if all([a in self.groupe_noeud_route_2_chaussees(100).id_left.tolist() for a in self.noeuds_uniques]) and self.nb_lgn>1 and self.nb_noeuds_uniques>2 :
                return True
            else : 
                if self.rd_pt_flag :
                    if self.nb_lgn_ss_rdpt<=self.nb_noeuds_simplifie-1 :
                        return True
                    else : 
                        return False
                else: return False
        elif (self.df_lign_fin_tronc_simplifie.rgraph_dbl==1).all() : 
            return True
        else :
            return True
    
    
class PasDeTraficError(Exception):
    """
    Exception levée si la ligne n'est pas affectée à du trafic
    """
    def __init__(self, idtroncon):
        Exception.__init__(self,f'pas de trafic sur le troncon : {idtroncon}')
        self.erreur_type='PasDeTraficError'  

class PasCorrespondanceError(Exception):
    """
    Exception levée si la ligne rhv n'est pas associee a une ligne MMM
    """
    def __init__(self, ident_rhv):
        Exception.__init__(self,f'pas de correspondance MMM pour la(es) ligne : {ident_rhv}')

class PasDeTraficInconnuError(Exception):
    """
    Exception levée si la ligne n'est pas affectée à du trafic
    """
    def __init__(self):
        Exception.__init__(self,"pas de trafic inconnu au noeud, normalement l'erruer est en amont")
        self.erreur_type='PasDeTraficInconnuError' 





