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

"""#############################################################
PARTIE ESTIMATION POP ET EMPLOI
############################################################"""

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
        initialiser le(s) premier(s) trajet(s) possible selon la ligne de d�part 
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
        Si une ligne se s�pare en plusieurs, il faut allonger le trajet existant et en cree un ou des nouveaux avec les diff�rentes possibilit�s
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
        limiter les trajets � ceux qui touchent une ligne de catg�orie 1,2,3 
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
        fonction globale de r�cup�ration de lal iste des idents composants le trajet le plus court, pour un b�timent
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
        points : liste de points ordonn�s
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
        savoir si l'arrive est liee � une voie de cat 1,2,3 ou impasse
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

