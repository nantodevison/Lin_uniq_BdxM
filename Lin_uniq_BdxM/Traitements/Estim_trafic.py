# -*- coding: utf-8 -*-
'''
Created on 16 dec. 2019

@author: martin.schoreisz

Module de ventilation des trafics issus des points de comptages
'''

import pandas as pd
from Outils import plus_proche_voisin,nb_noeud_unique_troncon_continu,verif_index
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
    noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['tmjo_2_sens'])>2 and x['nb_nan'][-99]<len(x['tmjo_2_sens']) and x['nb_nan'][-99]!=0 else False,axis=1)
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
    df_noeud=df_noeud_tot.loc[df_noeud_tot['noeud']==noeud].merge(df_temp[['ident','id_ign','type_noeud', 'idtronc']], on='id_ign')
    return df_noeud.drop_duplicates()

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
                    else : return 1
                elif nb_lgn_tch_noeud_unique == 2 :
                    return 1
            else : return 0
        else: return 1
    df.rgraph_dbl.fillna(1,inplace=True)
    df['list_noeud_unique']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).noeuds_uniques, axis=1) #liste des noeuds en fin de troncon['list_noeud_unique']=df_calcul.apply(lambda x : rt.Troncon(df_tot, x['idtronc']).noeuds_uniques, axis=1) #liste des noeuds en fin de troncon
    df['nb_noeud_unique']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_noeuds_uniques, axis=1) #nb de noeud en fin de troncon
    df['nb_noeud']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_noeuds, axis=1) #nb de noeud en fin de troncon
    df['nb_lgn_tch_noeud_unique']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_lgn_tch_noeud_unique, axis=1) 
    df['nb_lgn']=df.apply(lambda x : Troncon(df_tot, x['idtronc']).nb_lgn, axis=1)
    #ajout d'un attribut supplémentaire
    df['rgraph_dbl_2']=df.apply(lambda x : nb_sens(x['rgraph_dbl'],x['nb_noeud_unique'],x['nb_noeud'],x['nb_lgn_tch_noeud_unique'],x['nb_lgn']), axis=1)

def trouver_noeud_parrallele(Troncon,graph, noeud,distance):
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
    corresp_noeud_uniq=Troncon.groupe_noeud_route_2_chaussees(graph,distance)
    try :
        noeud_parrallele=corresp_noeud_uniq.loc[corresp_noeud_uniq['id_left']==noeud].id_right.values[0]
        return noeud_parrallele
    except IndexError : 
        return None
    

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
            noeud_parrallele=trouver_noeud_parrallele(Troncon(df_rdpt_simple,t),graph, num_noeud,100)
            if not noeud_parrallele : #ca veut dire qu'une 2 fois 2 voies se termine de par et d'autre par un rd pt
                continue
            #trouver les troncon qui intersectent ce troncon sur le debut ou la fin et qui n'ont pas été fléchés au début
            df_troncons_sup=df_tot.loc[((df_tot['source']==noeud_parrallele) | (df_tot['target']==noeud_parrallele)) & 
                                   (~df_tot.idtronc.isin(df_troncon_noeud.idtronc.tolist()))].idtronc.tolist()
            df_troncon_noeud_sup=df_noeud_troncon(df_rdpt_simple, noeud_parrallele,df_noeuds)
            df_troncon_noeud_sup=df_troncon_noeud_sup.loc[df_troncon_noeud_sup.idtronc.isin(df_troncons_sup)].copy()
            if df_troncon_noeud_sup.empty : 
                continue
            verif_double_sens(df_troncon_noeud_sup,df_tot)
            df_tronc_finale=pd.concat([df_troncon_noeud,df_troncon_noeud_sup], axis=0, sort=False).drop_duplicates('idtronc')
        try :
            return df_tronc_finale
        except UnboundLocalError : 
            return df_troncon_noeud
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
    
    if df.reset_index().duplicated('index').any() : #inon on peut avoir une errur si le numero d'index est dupliqué
        df=df.reset_index().drop('index',axis=1)
    df.loc[df['tmjo_2_sens']==-99,'tmjo_2_sens']=df.apply(lambda x : calcul_trafic_manquant_3troncons(
        df), axis=1)
        
def matrice_troncon_noeud_rhv(df_troncon_noeud,lgn_rdpt) :
    """
    df des lignes arrivant sur un noeud, avec la ligne de départ en face
    in : 
        df_troncon_noeud : df issue de carac_troncon_noeud()
        lgn_rdpt : df des lignes appartenenat a un rond point. issu de la dmearche interne de simplification des troncon
    out : 
        tab_corresp_rhv : la df issue du cross join des lignes de df_troncon_noeud, filtree
    """
    #filtrer les lignes des rd pt
    df_troncon_noeud2=df_troncon_noeud.loc[~df_troncon_noeud.ident.isin(lgn_rdpt.ident.tolist())].copy()
    cross_join=df_troncon_noeud2.assign(key=1).merge(df_troncon_noeud2.assign(key=1),on="key").drop("key", axis=1)
    cross_join['filtre']=cross_join.apply(lambda x : tuple(sorted((x['ident_x'],x['ident_y']))),axis=1)
    cross_join.drop_duplicates('filtre', inplace=True)
    tab_corresp_rhv=cross_join.loc[cross_join['ident_x']!=cross_join['ident_y']].drop('filtre',axis=1).copy()[['ident_x',
                'ident_y','idtronc_x','idtronc_y','tmjo_2_sens_x','tmjo_2_sens_y','cat_rhv_x','cat_rhv_y']]
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

def trafic_mmm(trafic_inconnus_prior_cat,mmm_simple,noeud, graph,df_rdpt_simple,cle_mmm_rhv):
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
        trouver le tmjo ode reference du mmm pour le calcul des trafic manquants,si plueisuers lignes possible
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
    
    def ajout_tmja(idtronc, tmja_base, noeud) : 
        """
        Pour les voies du MMM qui sont représentées par 2 lignes il faut ajouter les 2 trafics pour le troncon
        """
        troncon=Troncon(df_rdpt_simple,idtronc)
        noeud_parrallele=trouver_noeud_parrallele(troncon,graph, noeud,100)
        #trouver les lignes du troncon qui intersectent ce noeud
        lign_fin=troncon.df_lign_fin_tronc.loc[(troncon.df_lign_fin_tronc['source']==noeud_parrallele) | (troncon.df_lign_fin_tronc['target']==noeud_parrallele)]
        #jointure avec les données MMM
        trafc_mmm_sup=lign_fin[['idtronc','ident']].merge(cle_mmm_rhv[['NO','ident']], on='ident').merge(mmm_simple[['NO','tmja_tv']], on='NO')
        if trafc_mmm_sup.empty : 
            return tmja_base
        else : 
            return tmja_base+trafc_mmm_sup.tmja_tv.sum()
    
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
    #mise a jour des trafic mmm si 2*2voies MMM
    if traf_mmm.apply(lambda x : ','.join([str(x['idtronc_x']),str(x['idtronc_y'])]),axis=1).nunique()!=len(traf_mmm) :
        traf_mmm['tmja_tv_x']=traf_mmm.apply(lambda x : ajout_tmja(x['idtronc_x'], x['tmja_tv_x'], noeud),axis=1)
        traf_mmm['tmja_tv_y']=traf_mmm.apply(lambda x : ajout_tmja(x['idtronc_y'], x['tmja_tv_y'], noeud),axis=1)
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





