# -*- coding: utf-8 -*-
'''
Created on 24 avr. 2020

@author: martin.schoreisz
Module pour traiter les voies de categorie 4 du RHV : 
- partie association de comptage
- partie MMM
- partie estimation pop et emplois
'''

import geopandas as gp
import pandas as pd
import numpy as np
from copy import copy, deepcopy
from Outils import gp_changer_nom_geom
from shapely.ops import nearest_points
from collections import Counter

"""#############################################################
PARTIE POINT DE COMPTAGE
############################################################"""

def isolerDonneesCat4(gdf_rhv_groupe,gdf_rhv_groupe_123, affect_finale,lgn_proche_perm):
    """
    a partir des donnes generale et de categorie 4, isoler les donnees cat4
    in :
        gdf_rhv_groupe : l'enseble du rhv avec id_troncon
        gdf_rhv_groupe_123 : dataframe des donnees rhv de cat 1,2,3 traitees en amont cf modules estim_trafic et comptages
        affect_finale : df de regroupement des points de comptages ponctuels cf modules comptages
        lgn_proche_perm : dataframe des lignes proches de comptages permanents. cf modules comptages
    out : 
        rhv_grp_hors_123 : rhv hors donnees precedemment traitees
        affect_finale_cat4 : regroupement des compteurs ponctuels sur voies de cateorie 4
        cpt_perm_cat4 : donnes de compteurs permanents cat4
    """
    #voies rhv non comprises dans cat 1,2,3. Il faut une petite astuce car des pb de detremination de stroncons elementaires : 
    #on cherche les ident des lignes contenu dans les cat1,2,3, on prend les idtronc correspondant, et on cherche celles qui ne sont pas relatives à ces id
    rhv_grp_hors_123=gdf_rhv_groupe.loc[~gdf_rhv_groupe.idtronc.isin(gdf_rhv_groupe.loc[gdf_rhv_groupe.ident.isin(gdf_rhv_groupe_123.ident.tolist())].idtronc.to_list())]
    #points de comptages concernes par cat4
    affect_finale_cat4=affect_finale.loc[affect_finale['idtronc'].isin(rhv_grp_hors_123.idtronc.tolist())]
    #comptage permannet relatifs aux cat 4 (attention, certains sont a filtrer car trop proche d'une cat 1,2,3 et cat4, dc confusion)
    cpt_perm_cat4=lgn_proche_perm.loc[(lgn_proche_perm['ident_lgn'].isin(rhv_grp_hors_123.ident.tolist())) &
                                      (lgn_proche_perm['ident_x']!='Z21DE')][['ident_x', 'idtronc','mjo_val','rgraph_dbl','ident_lgn']].rename(
        columns={'mjo_val':'tmjo_tv','ident_x':'ident_cpt' })
    cpt_perm_cat4['type_cpt']='permanent'
    return rhv_grp_hors_123,affect_finale_cat4,cpt_perm_cat4

def jointureTraficLigne(rhv_grp_hors_123,affect_finale_cat4,cpt_perm_cat4):
    """
    a partir des donnees dentree de isolerDonneesCat4, ramener les points vers les lignes
    in :
        rhv_grp_hors_123 : rhv hors donnees precedemment traitees
        affect_finale_cat4 : regroupement des compteurs ponctuels sur voies de cateorie 4
        cpt_perm_cat4 : donnes de compteurs permanents cat4
    out : 
        groupe_ident_cpt : df des points de comptages regroupes par ident de ligne
        cpt_cat4_uniq_sens_dbl : df des points de groupe_ident_cpt avec un seul point de comptage et double sens
        cat4_lgn_pb : df des points qui vont poser soucis
    """
    #il faut vérifier que pour chaque troncon, les comptages sont bien au nombre de deux, ou concerne une voie unique. pour les autres on multiplie par ddeux la valeur de trafic 
    #jointure entre les comptage et ligne spour savoir si un comptage isolé a été réalise sur un sens uniq ou juste dans un seul sens pour une voie double sens et fusion avec les permanents
    joint_ctp_lgn=affect_finale_cat4.merge(rhv_grp_hors_123[['idtronc', 'ident', 'rgraph_dbl']], on='idtronc').rename(columns={'ident_x':'ident_cpt', 'ident_y' : 'ident_lgn'})
    joint_ctp_lgn['type_cpt']='ponctuel'
    joint_ctp_lgn_tot=pd.concat([cpt_perm_cat4,joint_ctp_lgn],axis=0, sort=False)
    #on regroupe par idtronc pour isoler les cas ou 1 seul ident_cpt et rgraph_dbl==1. Ceux la il faudra multiplier les trafic par deux
    groupe_ident_cpt=joint_ctp_lgn_tot.groupby('idtronc').agg({'tmjo_tv' : lambda x : set(x),'ident_cpt' : lambda x : set(x), 'ident_lgn' : lambda x : set(x),'rgraph_dbl': lambda x : set(x)} )
    cpt_cat4_uniq_sens_dbl=groupe_ident_cpt.loc[groupe_ident_cpt.apply(lambda x : len(x['ident_cpt'])== 1 and all([a==1 for a in x['rgraph_dbl']]), axis=1)]
    #il y a aussi le cas pbmatq des lignes à sens uniq avec 2 points de compatge, parfois liés à des erreurs rhv (idtronc 1320, 2920), ou des "2*2" voies
    cat4_lgn_pb=groupe_ident_cpt.loc[groupe_ident_cpt.apply(lambda x : len(x['ident_cpt'])==2 and all([a==0 for a in x['rgraph_dbl']]), axis=1)]
    #cat4_lgn_pb_list_idtronc_sens_uniq=[3094, 4687, 8724, 10025]
    return groupe_ident_cpt,cpt_cat4_uniq_sens_dbl,cat4_lgn_pb
    
def calculTraficPointComptage(rhv_grp_hors_123,groupe_ident_cpt,cpt_cat4_uniq_sens_dbl,cat4_lgn_pb,cat4_lgn_pb_list_idtronc_sens_uniq):
    """
    pour chaque ligne du rhv concernes calculer le trafic issu des points de comptage
    in : 
         rhv_grp_hors_123 : rhv hors donnees precedemment traitees
        groupe_ident_cpt : df des point de comptage groupes par ident de ligne rhv, cf jointureTraficLigne()
        cpt_cat4_uniq_sens_dbl : df des points de groupe_ident_cpt avec un seul point de comptage et double sens, cf jointureTraficLigne()
        cat4_lgn_pb : df des points qui vont poser soucis, cf jointureTraficLigne()
        cat4_lgn_pb_list_idtronc_sens_uniq list des ident du rhv a sens uniq, cf jointureTraficLigne()
    out : 
        rhv_grp_hors_123_traf : df des trafic sur rhv cat 4 
    """
    #calcul des tmjo_2_sens
    def calcul_tmjo_2sens(idtronc, list_1cpt_2sens, df_1ou2sens_2cpt, list_1sens, set_tmjo_tv, set_ident_cpt) : 
        if idtronc not in list_1cpt_2sens+df_1ou2sens_2cpt.index.tolist() :
            return sum(set_tmjo_tv) 
        elif idtronc in list_1cpt_2sens : 
            return list(set_tmjo_tv)[0]*2
        elif idtronc in df_1ou2sens_2cpt.index.tolist() : 
            if idtronc not in list_1sens : 
                return sum(set_tmjo_tv)
            else : 
                return max(set_tmjo_tv)
        else : return -99   
        
    def type_cpt(id_cpt_exp) : 
        if pd.isnull(id_cpt_exp) : return np.NaN
        elif id_cpt_exp[0]=='Z' : return 'permanent'
        else  : return 'ponctuel'

    groupe_ident_cpt.reset_index(inplace=True)
    groupe_ident_cpt['tmjo_2_sens']=groupe_ident_cpt.reset_index().apply(lambda x : calcul_tmjo_2sens(x['idtronc'],
                                                                                                      cpt_cat4_uniq_sens_dbl.index.tolist(),
                                                                                                      cat4_lgn_pb,cat4_lgn_pb_list_idtronc_sens_uniq,
                                                                                                      x['tmjo_tv'], x['ident_cpt']), axis=1)
    groupe_ident_cpt['id_cpt_exp']=groupe_ident_cpt.apply(lambda x : ', '.join([str(a) for a in x['ident_cpt']]), axis=1)
    #jointure avec les donnees hors cat1,2,3
    rhv_grp_hors_123_traf=rhv_grp_hors_123.merge(groupe_ident_cpt[['idtronc','tmjo_2_sens','id_cpt_exp']],on='idtronc', how='left').drop('id_y', axis=1).rename(columns={'id_x':'id'})
    rhv_grp_hors_123_traf=gp_changer_nom_geom(rhv_grp_hors_123_traf, 'geom')
    rhv_grp_hors_123_traf['type_cpt']=rhv_grp_hors_123_traf.id_cpt_exp.apply(lambda x : type_cpt(x) )
    return  rhv_grp_hors_123_traf

"""#############################################################
PARTIE ESTIMATION POP ET EMPLOI
############################################################"""

def importDonneesBase(fichierTraf1234,fichierVoiesNonRenseignees,fichierBati,fichierCorrespondanceBati):
    """
    importer les donnes de trafic et de bati necessaires aux traitements : 
    in : 
        fichierTraf1234 : fichier shape comprenant les trafic sur le rhv pour les voies de toutes categories,
        fichierVoiesNonRenseignees : chemin ver le fichier shape comrenant tout les lgnes du rhv non renseignees,
        fichierBati : fichier shape du bati, issu du LCSQA,
        fichierCorrespondanceBati : fichier de correspondance entre un id batiment les ident lignes du rhv, obtenu par coreesp_bati=plus_proche_voisin(bati,graph_filaire,200,'ID','ident') (attention c long)
    out : 
        gdf_traf_1234 : df du fichier entre, 
        voies_nc : df des voies a renseigner , 
        bati  df du fichier entre: ,
        coreesp_bati2 : df de correspondancenettoyee
    """
    gdf_traf_1234=gp.read_file(fichierTraf1234)
    #importdes voies non connues
    voies_nc=gp.read_file(fichierVoiesNonRenseignees)
    voies_nc=pd.concat([voies_nc,gdf_traf_1234.loc[gdf_traf_1234.cat_rhv.isin(['4','64'])].rename(columns={'id':'id_x'})[voies_nc.columns[:-1]]],axis=0, sort=False)
    voies_nc['longueur']=voies_nc.geometry.length
    bati=gp.read_file(fichierBati)
    #plus proche voisin sur la base de tout le rhv route
    ident_connus=gdf_traf_1234.ident.to_numpy()
    #coreesp_bati=plus_proche_voisin(bati,graph_filaire,200,'ID','ident')
    coreesp_bati2=pd.read_csv(fichierCorrespondanceBati)
    #la corresppondance peut renvoyer plusieurs valeurs si un vertex commun a plusieurs lignes est le point le plus proche. Dans ce cas on conserve l'ident d'une voies de cat1,2,3 si il y en a une, sionon n'importe lequel
    for i in coreesp_bati2.loc[coreesp_bati2.duplicated('ID')].ID.unique() :
        idents_dbl=coreesp_bati2.loc[coreesp_bati2['ID']==i].ident.tolist()
        mask=[str(i) in ident_connus for i in idents_dbl]
        if any(mask) : 
            ident_final=str(np.array(idents_dbl)[mask][0])
        else : 
            ident_final=str(idents_dbl[0])
        coreesp_bati2.loc[coreesp_bati2['ID']==i,'ident']=ident_final
    coreesp_bati2.drop_duplicates(['ID','ident'], inplace=True)
    coreesp_bati2['ident']=coreesp_bati2.ident.apply(lambda x : str(x))
    return gdf_traf_1234, voies_nc, bati,coreesp_bati2

def definitionVariablesBati(gdf_rhv_groupe,gdf_traf_1234, bati,coreesp_bati2):
    """
    perparer les donnees liées au batiment necessaires a la recherche de trajets
    in  : 
        graph_filaire : df du graph du filiare de voie : ca peut etre ameliorer : je me suis pris les pieds dans le tapis avec les sources et target des differents fcihiers
        gdf_rhv_groupe : df du rhv avec l'id troncon
        gdf_traf_123 cf importDonneesBase()
        bati : issu de importDonneesBase()
        voies_nc : issu de importDonneesBase()
        coreesp_bati2 :issu de importDonneesBase()
    out : 
        bati_ligne_proche : df d'association du bati et des idents
        
    """
    #ensuite, on va limiter les donées aux batiments relatifs aux voies non affectées
    list_bati_rhv_cat4=coreesp_bati2.loc[coreesp_bati2.ident.isin(gdf_traf_1234.loc[gdf_traf_1234.cat_rhv.isin(['4','64'])].ident.tolist())].ID.tolist()
    bati_voie_inconnue=bati.loc[bati.ID.isin(coreesp_bati2.loc[~coreesp_bati2.ident.isin(gdf_traf_1234.ident)].ID.tolist()+list_bati_rhv_cat4)].copy() # ça c'est pour avoir aussi le bati relatifs aux comptage voies cat 4 pour pouvoir comparer
    #definition des variables neecessaires
    #ramener la geometrie de la ligne la plus proche sur le bati inconnu
    bati_ligne_proche=bati_voie_inconnue.loc[(bati_voie_inconnue['PopT2016']>=1) & (bati_voie_inconnue['ID']!='BatiFictif')].merge(coreesp_bati2[['ID','ident']], on='ID')
    bati_ligne_proche['ident']=bati_ligne_proche.ident.apply(lambda x : str(x))
    bati_ligne_proche=bati_ligne_proche.merge(gdf_rhv_groupe[['ident', 'geometry']], on='ident').rename(columns={'geometry_x':'geom_p', 'geometry_y':'geom_l'})
    #calculer le point le plus proche situé sur la ligne
    bati_ligne_proche['point_proche']=bati_ligne_proche.apply(lambda x : nearest_points(x['geom_p'],x['geom_l'])[1],axis=1)
    return bati_ligne_proche

def definitionVariablesActivite(gdf_rhv_groupe,ppvActiviteRhv,nafListen5, fichierEtablissementBdxMet, fichierUnitesLegalesBdxMet,fichierTrancheEffectif):
    """
    Preparer les variables d'activites allant serviur au calcul des trajets
    in : 
        fichierEtablissementBdxMet : fichier des etablissement sur la zone de Bdx Met
        fichierUnitesLegalesBdxMet : fichier des unites legales sur la zone de BdxMet
        ppvActiviteRhv : ident de la ligne rhv plus proche voisin de l'activite
        nafListen5 : df des codes naf avec ajout d'une colonne perso sur les codes a exclure
        fichierTrancheEffectif : fichier perso d'association d'uen nombre arbitraire a une tranche d'effecteif
    out : 
        etablissementEnrichi : integralite des etablissement avec donnees sirene en plus
        effectifEtablissement : df avec geometrie d'etablissement et donnees d'effectif moyen, uniquement si il existe des effectif et que l'etablissement n'est pas fereme
        activ_ligne_proche : associations du point le plus proche de la voie sur l'ident la plus proche (preparatoire au calcul de trajet)
    """
    #isoler les activites le long des voies de categorie 4
    activiteRhvCat4=ppvActiviteRhv.loc[ppvActiviteRhv.ident_rhv.isin(gdf_rhv_groupe.loc[gdf_rhv_groupe.cat_rhv.isin(('4','64'))].ident.tolist())].copy()
    #donnees d'effectifs
    etablissements=pd.read_csv(fichierEtablissementBdxMet)[['siren','nic','siret','etatAdministratifEtablissement','trancheEffectifsEtablissement','anneeEffectifsEtablissement','etablissementSiege','activitePrincipaleEtablissement','caractereEmployeurEtablissement']]
    UniteLegale=pd.read_csv(fichierUnitesLegalesBdxMet)[['siren','trancheEffectifsUniteLegale','anneeEffectifsUniteLegale','categorieEntreprise','etatAdministratifUniteLegale','activitePrincipaleUniteLegale','caractereEmployeurUniteLegale']]
    etablissementEnrichi=activiteRhvCat4.assign(ident=activiteRhvCat4.ident.astype('float')).merge(etablissements, left_on='ident', right_on='siret', how='left').merge(UniteLegale, on='siren', how='left')
    #filtre sur les effectifs inconnus ou à 0
    etablissementEffectifPositif=etablissementEnrichi.loc[((etablissementEnrichi.trancheEffectifsEtablissement.isna()) & (~etablissementEnrichi.trancheEffectifsUniteLegale.isna()) & (etablissementEnrichi.etablissementSiege) & ((~etablissementEnrichi['trancheEffectifsUniteLegale'].isin(['NN','00','0.0'])))) | 
                            ((~etablissementEnrichi['trancheEffectifsEtablissement'].isin(['NN','00','0.0'])) & (~etablissementEnrichi['trancheEffectifsEtablissement'].isna()) )].copy()
    #filtre sur les activités de constrcution et de transport
    etablissementEffectifPositif=etablissementEffectifPositif.loc[~etablissementEffectifPositif.activitePrincipaleEtablissement.isin(nafListen5.loc[nafListen5.ModifEffectif=='N'].Code.tolist())]
    etablissementEffectifPositifOuvert=etablissementEffectifPositif.loc[(etablissementEffectifPositif.etatAdministratifEtablissement!='F')].copy()
    #pour les établissement avec effectif : prendre la valeur d'éffectif de l'établissement, sinon celle de l'unité légale
    etablissementEffectifPositifOuvert['effecFinal']=etablissementEffectifPositifOuvert.apply(lambda x : x['trancheEffectifsUniteLegale'] if pd.isnull(x['trancheEffectifsEtablissement']) else x['trancheEffectifsEtablissement'], axis=1 )
    #ensuite on recupere l'affectation à un effectif moyen et on joint le tout
    descriptionEffectif=pd.read_csv(fichierTrancheEffectif)
    effectifEtablissement=etablissementEffectifPositifOuvert.merge(descriptionEffectif, left_on='effecFinal', right_on='codeEffectif',how='left')
    activ_ligne_proche=effectifEtablissement.rename(columns={'id':'ID','ident':'siren','ident_rhv':'ident'})
    activ_ligne_proche=activ_ligne_proche.merge(gdf_rhv_groupe[['ident', 'geometry']], on='ident').rename(columns={'geometry_x':'geom_p', 'geometry_y':'geom_l'})
    activ_ligne_proche['point_proche']=activ_ligne_proche.apply(lambda x : nearest_points(x['geom_p'],x['geom_l'])[1],axis=1)
    return etablissementEnrichi,effectifEtablissement,activ_ligne_proche

def definitionVariablesVoies(graph_filaire,gdf_traf_1234, voies_nc ):
    """
    preparer les données liées au voies pour la recherche de trajet
    in  : 
        graph_filaire : df du graph du filiare de voie : ca peut etre ameliorer : je me suis pris les pieds dans le tapis avec les sources et target des differents fcihiers
        gdf_traf_1234 : : donnes dentree, cf importDonneesBase() 
        voies_nc : donnes dentree, cf importDonneesBase()
    out : 
        ensemble_lignes : df des voies non connues avec reset de l'index
        vertex_connus : vertex des voies qui ne sont pas de cat 4 ou 64 et dont le cat_rhv est connu
        vertex_impasse : vertex des voies en impasses
    """
    ensemble_lignes=voies_nc.reset_index(drop=True)
    #j'ai besoin de cette ligne car les source_target de gdf_traf_1234 sont différents de ceux de ensemble_lignes
    gdf_tot_eq_1234=graph_filaire.loc[graph_filaire.ident.isin(gdf_traf_1234.loc[(~gdf_traf_1234.cat_rhv.isin(('4','64'))) & (~gdf_traf_1234.cat_rhv.isna())].ident.tolist())] #trouver tt les lignes qui ne sont pas cat4 ou sans valeur
    vertex_connus=list(set(gdf_tot_eq_1234.source.tolist()+gdf_tot_eq_1234.target.tolist()))
    vertex_impasse=[k for k, v in Counter(graph_filaire.source.tolist()+graph_filaire.target.tolist()).items() if v==1]
    return ensemble_lignes,vertex_connus, vertex_impasse

class ensemble_trajet : 
    def __init__(self,ligne_depart,point_depart, ensemble_lignes, vertex_connus, vertex_impasse, ensemble_vertex):
        self.ligne_depart,self.point_depart=ligne_depart,point_depart 
        self.ensemble_lignes, self.vertex_connus, self.vertex_impasse,self.ensemble_vertex=ensemble_lignes, vertex_connus, vertex_impasse, ensemble_vertex
        self.src, self.tgt, self.dist_src, self.dist_tgt, self.noeud_proche=self.calcul_noeud_proche()
    
    def calcul_noeud_proche(self):
        """
        trouver le vertex de la ligne de depart le plus proche du point de depart
        """
        src=self.ensemble_lignes.loc[self.ensemble_lignes.ident==self.ligne_depart].source.values[0]
        tgt=self.ensemble_lignes.loc[self.ensemble_lignes.ident==self.ligne_depart].target.values[0]
        dist_src=self.point_depart.distance(self.ensemble_vertex.loc[self.ensemble_vertex.id==src].geom.values[0])
        dist_tgt=self.point_depart.distance(self.ensemble_vertex.loc[self.ensemble_vertex.id==tgt].geom.values[0])
        noeud_proche=src if dist_src<dist_tgt else tgt
        return src, tgt, dist_src, dist_tgt, noeud_proche
            
    def initialiser_trajets(self, debug=False):
        """
        initialiser le(s) premier(s) trajet(s) possible selon la ligne de depart
        """
        lgn_depart=self.ensemble_lignes.loc[self.ensemble_lignes['ident']==self.ligne_depart]
        if lgn_depart.rgraph_dbl.all()==1 or debug : 
            self.liste_trajets_possibles=[trajet(self.ligne_depart,self.point_depart, self.ensemble_lignes, self.vertex_connus, self.vertex_impasse,self.ensemble_vertex, 
                                                 [v,],[self.ligne_depart,] )for v in lgn_depart.source.tolist()+lgn_depart.target.tolist()]
        else : 
            self.liste_trajets_possibles=[trajet(self.ligne_depart,self.point_depart, self.ensemble_lignes, self.vertex_connus, self.vertex_impasse,self.ensemble_vertex, 
                                                 [lgn_depart.target.values[0],],[self.ligne_depart,])]
        for t in self.liste_trajets_possibles : 
            t.calcul_statut()
    
    def creer_nouveau_trajet(self,t,ident_possibles, vertex_possibles):
        """
        Si une ligne se  separe  en plusieurs, il faut allonger le trajet existant et en cree un ou des nouveaux avec les differentes possibilites
        """
        new_t=deepcopy(t)#pour ne pas impacter les donnees de base
        for e,(i, v) in enumerate(zip(ident_possibles, vertex_possibles)) : 
            if e==0:
                t.points.append(v)
                t.lignes.append(i)
            else : 
                liste_base_point=deepcopy(new_t.points)#pour pouvoir revenir au vertex de d�part
                liste_base_point.append(v)
                liste_base_lignes=deepcopy(new_t.lignes)
                liste_base_lignes.append(i)
                self.liste_trajets_possibles.append(trajet(self.ligne_depart,self.point_depart, self.ensemble_lignes, self.vertex_connus, 
                                                           self.vertex_impasse,self.ensemble_vertex, liste_base_point,liste_base_lignes ))
    
    def allonger_trajet(self, debug=False):
        """
        chercher pour tous les trajets non arrive les nouvelles lignes a ajouter
        in : 
            debug : booleen : pour gerer le cas où on a trouver aucun, chemin, on suppose que c'est une erreur d'encodage de rgraph_dbl, et on passe tout lemonde à 1.
                              devrait pouvoir etre remplacer par un decorateur, cf fonction trajet.trouver_nouvelles_lignes
        """
        cpt_lg=0 #compteur mis en place car dans ipython l'utlisation de if lg_ref not in locals() remonte une UnboundLocalError
        if self.verifier_statut_tout_trajet():
            for t in self.liste_trajets_possibles :
                t.arrive_type()
        while not self.verifier_statut_tout_trajet() :
            for t in self.liste_trajets_possibles : 
                t.calcul_statut()                
                #on ajoute un break selon le nombre de points, pour �viter de prendre des trajets trop longs
                if len(t.points)>18 : 
                    t.statut='arrive'
                    t.type_arrivee='interrompu_TropVertex'
                    continue

                #si un troncon arrive a une ligne cat1,2,3, on se sert de la longueur totale (avec le troncon entier) comme ref pour limiter la recherche
                if t.statut == 'arrive':
                    if not t.type_arrivee : 
                        t.arrive_type()
                    if t.type_arrivee=='cat3' :
                        lg_t=t.calcul_longueur()
                        if cpt_lg==0 : 
                            lg_ref=deepcopy(lg_t)
                            cpt_lg+=1
                        else:
                            lg_ref=min(lg_ref,lg_t)
                            cpt_lg+=1
                else : 
                    try : 
                        ident_possibles,vertex_possibles=t.trouver_nouvelles_lignes(debug)
                    except PasDeVoiePossibleError:
                        t.statut='arrive'
                        t.type_arrivee='interrompu_pasDeVoie'
                        continue
                    if len(ident_possibles)==1 :
                        t.points+=vertex_possibles
                        t.lignes+=(ident_possibles)
                    else : 
                        self.creer_nouveau_trajet(t,ident_possibles, vertex_possibles)
                    if cpt_lg>0  : #limitation de la recherche par longueur
                        if t.calcul_longueur()>lg_ref : 
                            t.statut='arrive'
                            t.type_arrivee='interrompu_longueur'

    def verifier_statut_tout_trajet(self):
        """
        savoir si tous les trajets sont arrive ou non
        """
        return all([t.statut=='arrive' for t in self.liste_trajets_possibles])
    
    def isoler_trajet_cat3(self):
        """
        limiter les trajets  a  ceux qui touchent une ligne de catg�orie 1,2,3 
        """
        return self.df_tt_trajet.loc[self.df_tt_trajet['type_arrivee']=='cat3'].copy()
    
    def creer_df_tt_trajet(self) : 
        """
        creer un dico avec pour chaque trajet touchant une cat 1,2,3 : en key un id unique, en value un dico avec comme key points, lignes et longueur
        """
        self.df_tt_trajet=pd.DataFrame.from_dict({i: {'points':self.liste_trajets_possibles[i].points, 'lignes':self.liste_trajets_possibles[i].lignes, 
                                                     'longueur_tot':self.liste_trajets_possibles[i].calcul_longueur_pt_depart(),
                                                     'lg_hors_debut':self.liste_trajets_possibles[i].calcul_longueur_hors_debut(),
                                                    'type_arrivee' : self.liste_trajets_possibles[i].type_arrivee} 
         for i in range(len(self.liste_trajets_possibles))}, orient='index')
        
    def filtrer_df_tt_trajet(self):
        """
        filtrer la df_tt_trajet pour ne garder que le trajet le plus court pour chaque point de départ et d'arrivé pour les trajet de type_arrivee='cat3
        """
        df=self.isoler_trajet_cat3()
        df['pt_depart']=df.points.apply(lambda x : x[0])
        df['pt_arrive']=df.points.apply(lambda x : x[-1])
        self.df_trajet_cat3_grp_OD=df.loc[df['longueur_tot']==df.groupby(['pt_depart','pt_arrive']).longueur_tot.transform(min)].sort_values('pt_depart')
        self.debug=True if self.df_trajet_cat3_grp_OD.empty else False
        self.df_trajet_cat3_grp_OD['points']=self.df_trajet_cat3_grp_OD.points.apply(lambda x : tuple(x))
        self.df_trajet_cat3_grp_OD['lignes']=self.df_trajet_cat3_grp_OD.lignes.apply(lambda x : tuple(x))
        
    def calculs_trajets(self):
        """
        fonction globale de recuperation  de lal iste des idents composants le trajet le plus court, pour un batiment
        """
        self.initialiser_trajets()
        self.allonger_trajet()
        self.creer_df_tt_trajet()
        self.filtrer_df_tt_trajet()
        if self.debug : # dans le cas où self.df_trajet_cat3_grp_OD est vide, suppose que pb encodage rgraph_dbl, donc on recommence toute l'opération mais en considérant
                        #tout le monde en rgraph_dbl=1 dans fonction trouver_ligne.CE mm type de pb impacte aussi la def du trajet de base, cf fonctionc initialiser trajet
            self.initialiser_trajets()
            self.allonger_trajet(self.debug)
            self.creer_df_tt_trajet()
            self.filtrer_df_tt_trajet()
            if self.debug : 
                self.initialiser_trajets(self.debug)
                self.allonger_trajet()
                self.creer_df_tt_trajet()
                self.filtrer_df_tt_trajet()
                if self.debug : 
                    self.initialiser_trajets(self.debug)
                    self.allonger_trajet(self.debug)
                    self.creer_df_tt_trajet()
                    self.filtrer_df_tt_trajet()
        df_finale=self.df_trajet_cat3_grp_OD.loc[self.df_trajet_cat3_grp_OD.longueur_tot==self.df_trajet_cat3_grp_OD.longueur_tot.min()]
        if df_finale.empty : 
            raise PasDeCheminCat3Error(self.ligne_depart)
        self.trajet_court=tuple(df_finale.lignes.to_numpy()[0])
        self.point_arrive=df_finale.points.to_numpy()[0][-1]
    
    def maj_longueur_tot(self,df):
        """
        Dans le cas ou on recupere la df_trajet_cat3_grp_OD d'un autre ensemble de trajet, mettre a jour la longueur totale 
        """
        df['longueur_tot']=df.apply(lambda x : x['lg_hors_debut']+self.dist_src if x['pt_depart']==self.src else x['lg_hors_debut']+self.dist_tgt, axis=1)
        
    
class trajet(ensemble_trajet) : 
    def __init__(self, ligne_depart,point_depart, ensemble_lignes, vertex_connus, vertex_impasse,ensemble_vertex, point, lignes):
        """
        points : liste de points ordonnes
        lignes : list d'identifiant de lignes
        """
        self.points, self.lignes=point, lignes
        self.type_arrivee=None
        super().__init__(ligne_depart, point_depart,ensemble_lignes, vertex_connus, vertex_impasse,ensemble_vertex)
     
    def calcul_statut(self):
        """
        savoir si le trajets estarrive a son terme ou non
        """
        self.statut='arrive' if self.points[-1] in self.vertex_connus+self.vertex_impasse else 'en_cours'
        
    def arrive_type(self):
        """
        savoir si l'arrive est liee a une voie de cat 1,2,3 ou impasse
        """
        if not self.type_arrivee or self.type_arrivee=='None':
            self.type_arrivee='impasse' if self.points[-1] in self.vertex_impasse else 'cat3'
        
    def trouver_nouvelles_lignes(self, debug=False):
        """
        trouver les lignes possibles a parcourir, et les vertex associes, trier dans le mm ordre
        in : 
            debug : si le calcul des tronçons nedonne aucun type 'cat3', on refait tourner cette fonction avec param debug pour obtenir une liste des idents possibles différentes.
                devrait pouvoir etre rempacée par un décorateur
        """
        ident_tch=self.ensemble_lignes.loc[((self.ensemble_lignes['source']==self.points[-1]) | (self.ensemble_lignes['target']==self.points[-1])) & 
                                           (~self.ensemble_lignes.ident.isin(self.lignes)) &
                                           ~((self.ensemble_lignes.source.isin(self.points)) & (self.ensemble_lignes.target.isin(self.points))) ].copy()
        if not debug :                             
            ident_possibles=ident_tch.loc[(ident_tch['rgraph_dbl']==1) | ((ident_tch['rgraph_dbl']==0) & (self.ensemble_lignes['target']!=self.points[-1]))].copy()
        else : 
            ident_possibles=ident_tch.copy()
        if ident_possibles.empty : #cas par exemple d'une route en impasse d'un cot�, au debut
            raise PasDeVoiePossibleError(self.points[-1], self.lignes[-1])
        ident_possibles['vertex']=ident_possibles.apply(lambda x : x['source'] if x['source']!=self.points[-1] else x['target'], axis=1)
        vertex_possibles=ident_possibles.vertex.tolist()
        lignes_possibles=ident_possibles.ident.tolist()
        return lignes_possibles,vertex_possibles 
    
    def calcul_longueur_hors_debut(self):
        """
        calcuul de la longueur du trajet, sans le premier troncon
        """
        return self.ensemble_lignes.loc[(self.ensemble_lignes['ident'].isin(self.lignes[1:]))].longueur.sum()
    
    def calcul_longueur_pt_depart(self) : 
        """
        longueur du trajet parrapport au point de l'ident de depart le plus proche du bati 
        """
        if self.src==self.points[0] : 
            return self.dist_src+self.calcul_longueur_hors_debut()
        else : return self.dist_tgt+self.calcul_longueur_hors_debut()
        """return (self.point_depart.distance(self.ensemble_vertex.loc[self.ensemble_vertex['id']==self.points[0]].geom.values[0])+
                self.ensemble_lignes.loc[(self.ensemble_lignes['ident'].isin(self.lignes[1:]))].longueur.sum())"""
    
    def calcul_longueur(self):
        """
        longueur e prenant en compte la totalité de le laongueur de l'ident de depart
        """
        #self.calcul_longueur_pr_depart()+
        return self.ensemble_lignes.loc[(self.ensemble_lignes['ident'].isin(self.lignes))].longueur.sum()
    
    
class PasDeVoiePossibleError(Exception):
    """
    Erreur lev�e si il n'y a pas de voies a emprunter. Exemeple : premier vertex est le vertex d'une impasse
    """
    def __init__(self, points, lignes):
        Exception.__init__(self,f'pas de lignes de trajet possible sur le(s) vertex : {points} de(s) ligne(s) {lignes}')
        
class PasDeCheminCat3Error(Exception):
    """
    Erreur lev�e si il n'y a pas de voies a emprunter. Exemeple : premier vertex est le vertex d'une impasse
    """
    def __init__(self, ligne):
        Exception.__init__(self,f'pas de chemin possible vers une cat3 depuis la ligne {ligne}')

