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
    out : 
        noeud_grp : df des noeud avec des tuples issu des voies entrantes pour : le tmjo, la cat_rhv, le nb de NaN, sic'est estimable ou non (édfinition variable
                    selon type_carr
        df_noeuds : dataframe de tout les noeuds            
    """
    #1. faire la liste des lignes et des noeuds
    df_noeuds=pd.concat([df_trafic[['id_ign','source','tmjo_2_sens','cat_rhv','rgraph_dbl']].rename(columns={'source':'noeud'}),
                            df_trafic[['id_ign','target','tmjo_2_sens','cat_rhv','rgraph_dbl']].rename(columns={'target':'noeud'})],axis=0,sort=False)
    df_noeuds.tmjo_2_sens.fillna(-99,inplace=True)
    #2. Trouver les noeuds qui comporte au moins une valeur connue. On y affecte une valuer True dans un attribut drapeau 'estimable'
    noeud_grp=df_noeuds.groupby('noeud').agg({'tmjo_2_sens':lambda x : tuple(x),'cat_rhv':lambda x : tuple(x)}).reset_index()
    noeud_grp['nb_nan']=noeud_grp.tmjo_2_sens.apply(lambda x : Counter(x))
    if type_carr == 'max 1 NaN' :
        noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['nb_nan'])>2 and x['nb_nan'][-99]==1 else False,axis=1)
    elif type_carr == 'min 1 ok' : 
        noeud_grp['estimable']=noeud_grp.apply(lambda x : True if len(x['nb_nan'])>2 and x['nb_nan'][-99]<len(x['nb_nan']) and x['nb_nan'][-99]!=0 else False,axis=1)
    return noeud_grp, df_noeuds

def trouver_noeud_3tronc_1NaN(df_traf_finale, liste_noeud_traites):
    """
    trouver les noeuds au centre de 3troncons dont 1 seul à 1 rtafic inconnu
    in : 
        df_traf_finale : dataframe du filaire de voie avec rond point simplifie et idtroncon, qui va etre mise à jour en trafic ensuite
        liste_noeud_traites : liste des noeuds ayant fait l'objet d'une tentive de rensignement et qui ont foires
    out : 
        liste_noeuds_a_traiter : liste des noeuds a passer à la fonction de aclcul
    """
    #trouver les noeuds fv concernes par des lignes à mettre à jour
    noeud_grp,df_noeud_tot=noeud_fv_ligne_ss_trafic(df_traf_finale,'max 1 NaN')
    #se limiter aux noeuds présentant 3 lignes (pour rappel, seul une ligne présente un trafic inconnu)
    df_noeud_3tronc=noeud_grp.loc[(noeud_grp['estimable']) & 
                 (noeud_grp.apply(lambda x : len(x['tmjo_2_sens'])==3, axis=1)) & (~noeud_grp.noeud.isin(liste_noeud_traites))]
    liste_noeud=df_noeud_3tronc.noeud.tolist()
    return liste_noeud,df_noeud_tot
    

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

def verif_carrefour_2_chaussees(df, df_rdpt_simple, df_tot, graph,num_noeud):
    """
    verifier que les voies représetées par 2 lignes ne croise bien que 2 autres troncons. 
    Cette vérif est due à l'analyse par noeud : pour une 2*2 voies il y a 4 noeuds de fin, dc on pourrait louper des lignes
    in : 
        df : df des troncon arrivant sur un noeud, issu de verif_double_sens()
        df_rdpt_simple : ataframe du filaire de voie avec rond point simplifie et idtroncon
        df_tot : 
        graoh : table des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
        num_noeud : integer : numero du noeud du carrefour
    """
    if (df.loc[df['maj']].rgraph_dbl==0).all() and (df.loc[df['maj']].rgraph_dbl_2==1).all() : 
        #trouver le troncon à mettre à jour :
        troncon=Troncon(df_rdpt_simple,df.loc[df['maj']].idtronc.values[0])
        #sutilisation de la cle de correspondance entre les neouds
        corresp_noeud_uniq=troncon.groupe_noeud_route_2_chaussees(graph,100)
        noeud_parrallele=corresp_noeud_uniq.loc[corresp_noeud_uniq['id_left']==num_noeud].id_right.values[0]
        #trouver les troncon qui intersectent ce troncon sur le debut ou la fin et qui n'ont pas été fléchés au début
        df_troncons_sup=df_tot.loc[((df_tot['source']==noeud_parrallele) | (df_tot['target']==noeud_parrallele)) & 
                               (~df_tot.idtronc.isin(df.idtronc.tolist()))].copy()
        if not df_troncons_sup.empty : 
            raise PasDeTraficError(troncon.id)

def separer_troncon_a_estimer(df):
    """
    isoler les troncon de trafic inconnu sur la base du tmjo_2_sens
    in : 
        df : dataframe des voies arrivants à un noeud, issu de df_noeud_troncon()
    """
    return df.loc[df['tmjo_2_sens']!=-99], df.loc[df['tmjo_2_sens']==-99]

def calcul_trafic_manquant_3troncons(num_noeud, df,df_rdpt_simple, df_tot, nom_idtroncon, graph) : 
    """
    detreminer du trafic sur unnoeud a 3 voies avec 2 trafic connu
    in : 
        num_noeud : identifiant du noeud
        df : df des lignes liées au noeud
        df_rdpt_simple : dataframe du filaire de voie avec rond point simplifie et idtroncon
        df_tot : df globale du reseau routier sans simplification des rd points. doit contenir un attribut de groupement par troncon
        nom_idtroncon : string : nom de l'attribut de groupement par troncon
        graph : table des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
    """
     
    #vérif / mise en forme
    df_calcul=df.copy()
    verif_double_sens(df_calcul,df_tot) #ajouter un deuxieme attribuit decrovant le double sens
    verif_carrefour_2_chaussees(df_calcul,df_rdpt_simple, df_tot, graph,num_noeud) #leve une erreur siplus de 3 troncon aux carrefour (du au 2*2 voies)
    #print(df_calcul)
    df_calcul_trafic_exist,df_calcul_trafic_null=separer_troncon_a_estimer(df_calcul)
    
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
            return df_calcul_trafic_exist.loc[df_calcul_trafic_exist['rgraph_dbl']==1].tmjo_2_sens.values[0]

def maj_carrefour_3_troncons(liste_noeud,df_tot,df_rdpt_simple,df_noeud_tot,graph):
    """
    mettre a jour des valuers de trafic pour les troncons arrivant sur des carrefours a 3 troncnons
    in : 
        liste_noeud : liste des noeuds a traiter
        df_tot : df globale du reseau routier sans simplification des rd points. doit contenir un attribut de groupement par troncon, source, target, tmja 
                issu des points de comptage, etc.
        df_rdpt_simple : dataframe du filaire de voie avec rond point simplifie et idtroncon
        df_noeud_tot : dataframe de l'ensemnle des noeuds, issus de noeud_fv_ligne_ss_trafic()
        graph : df des vertex du graph sans modif liées au rond points. cf classe Troncon fonction groupe_noeud_route_2_chaussees()
    out : 
        dico_trafic_troncon : dico des trafic par troncon. cle : troncon, value :trafic
        dico_noeud_pb : dico des oeuds sans trafic, en cle le numero de noeud
    """
    dico_trafic_troncon,dico_noeud_pb={},{}
    for noeud in liste_noeud :
        print(noeud)
        df_noeud=df_noeud_troncon(df_rdpt_simple,noeud,df_noeud_tot)
        df_noeud['maj']=df_noeud.apply(lambda x : True if x['tmjo_2_sens']==-99 else False, axis=1)
        troncon_noeud=df_noeud.loc[df_noeud['maj']].idtronc.values[0]
        try : 
            df_noeud.loc[df_noeud['maj'],'tmjo_2_sens']=df_noeud.apply(lambda x : calcul_trafic_manquant_3troncons(
                noeud,df_noeud,df_rdpt_simple,df_tot,'idtronc',graph), axis=1)
        except PasDeTraficError :
            dico_noeud_pb[noeud]=(troncon_noeud,'PasDeTraficError')
            continue
        except IndexError : 
            dico_noeud_pb[noeud]=(troncon_noeud,'IndexError')
            continue
        if df_noeud.tmjo_2_sens.isnull().any() : 
            dico_noeud_pb[noeud]=(troncon_noeud,'autre Pb trafic')
            continue
        dico_trafic_troncon[df_noeud.loc[df_noeud['maj']].idtronc.values[0]]=df_noeud.loc[df_noeud['maj']].tmjo_2_sens.values[0]
    
    return dico_trafic_troncon, dico_noeud_pb


  
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
    
    
    
    
    
    