/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.oasis.core.services.api.beans;

import io.github.oasis.core.Game;
import io.github.oasis.core.ID;
import io.github.oasis.core.TeamMetadata;
import io.github.oasis.core.UserMetadata;
import io.github.oasis.core.collect.Pair;
import io.github.oasis.core.elements.RankInfo;
import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.elements.SimpleElementDefinition;
import io.github.oasis.core.exception.OasisDbException;
import io.github.oasis.core.exception.OasisException;
import io.github.oasis.core.exception.OasisRuntimeException;
import io.github.oasis.core.external.Db;
import io.github.oasis.core.external.DbContext;
import io.github.oasis.core.external.Mapped;
import io.github.oasis.core.external.OasisRepository;
import io.github.oasis.core.external.PaginatedResult;
import io.github.oasis.core.model.*;
import io.github.oasis.core.services.SerializationSupport;
import io.github.oasis.core.utils.Numbers;
import io.github.oasis.core.utils.Texts;
import io.github.oasis.core.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oasis.core.utils.Constants.COMMA;

/**
 * @author Isuru Weerarathna
 */
//@Component("redis")
public class RedisRepository
//        implements OasisRepository
{

//    private static final String ALL_PATTERN = "*";
//
//    public static final String ALL_RANKS_KEY = "ranks";
//
//    private final Db dbPool;
//    private final SerializationSupport serializationSupport;

//    public RedisRepository(@Qualifier("cache") Db dbPool, SerializationSupport serializationSupport) {
//        this.dbPool = dbPool;
//        this.serializationSupport = serializationSupport;
//    }

//    @Override
//    public EventSource addEventSource(EventSource eventSource) {
//        return withDbContext(db -> {
//            String token = eventSource.getToken();
//            if (existEventSource(token, db)) {
//                throw new OasisRuntimeException("A source token is already exist with same token!");
//            } else if (Objects.isNull(eventSource.getSecrets())
//                    || Objects.isNull(eventSource.getSecrets().getPrivateKey())
//                    || Objects.isNull(eventSource.getSecrets().getPublicKey())) {
//                throw new OasisRuntimeException("Source secrets must have defined!");
//            }
//
//            db.setValueInMap(ID.ALL_SOURCES, String.valueOf(eventSource.getId()), serializationSupport.serialize(eventSource));
//            updateSourceMetadata(eventSource, db);
//            return eventSource;
//        });
//    }
//
//    @Override
//    public EventSource deleteEventSource(int id) {
//        return withDbContext(db -> {
//            String valueFromMap = db.getValueFromMap(ID.ALL_SOURCES, String.valueOf(id));
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("No event source is found by given id!");
//            }
//
//            EventSource source = serializationSupport.deserialize(valueFromMap, EventSource.class);
//            db.removeKeyFromMap(ID.ALL_SOURCES, String.valueOf(id));
//            db.removeKeyFromMap(ID.ALL_SOURCES_INDEX, source.getToken());
//            return source;
//        });
//    }
//
//    @Override
//    public EventSource readEventSource(int id) {
//        return withDbContext(db -> {
//            String valueFromMap = db.getValueFromMap(ID.ALL_SOURCES, String.valueOf(id));
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("No event source is found by given id!");
//            }
//
//            return serializationSupport.deserialize(valueFromMap, EventSource.class);
//        });
//    }
//
//    @Override
//    public EventSource readEventSource(String token) {
//        return withDbContext(db -> {
//            String valueFromMap = db.getValueFromMap(ID.ALL_SOURCES_INDEX, token);
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("No event source is found by given token!");
//            }
//
//            EventSourceMetadata meta = serializationSupport.deserialize(valueFromMap, EventSourceMetadata.class);
//            String val = db.getValueFromMap(ID.ALL_SOURCES, String.valueOf(meta.getId()));
//            return serializationSupport.deserialize(val, EventSource.class);
//        });
//    }
//
//    @Override
//    public EventSourceSecrets readEventSourcePublicSecrets(int id) {
//        // Not allowed reading secrets from engine database or cache.
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public EventSourceSecrets readEventSourceSecrets(int id) {
//        // Not allowed reading secrets from engine database or cache.
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public List<EventSource> listAllEventSources() {
//        return withDbContext(db -> {
//            Map<String, String> all = db.MAP(ID.ALL_SOURCES).getAll();
//            if (all == null) {
//                return new ArrayList<>();
//            }
//
//            List<EventSource> sources = new ArrayList<>();
//            for (Map.Entry<String, String> entry : all.entrySet()) {
//                if (entry.getValue() != null) {
//                    sources.add(serializationSupport.deserialize(entry.getValue(), EventSource.class));
//                }
//            }
//            return sources;
//        });
//    }
//
//    @Override
//    public List<EventSource> listAllEventSourcesOfGame(int gameId) {
//        return listAllEventSources().stream()
//                .filter(sources -> sources.getGames() != null && sources.getGames().contains(gameId))
//                .collect(Collectors.toList());
//    }
//
//    @Override
//    public void addEventSourceToGame(int sourceId, int gameId) {
//        withDbContext(db -> {
//            String valueFromMap = db.getValueFromMap(ID.ALL_SOURCES, String.valueOf(sourceId));
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("No event source is found by given id!");
//            }
//
//            EventSource source = serializationSupport.deserialize(valueFromMap, EventSource.class);
//            if (source.getGames() != null) {
//                if (!source.getGames().add(gameId)) {
//                    throw new OasisRuntimeException("Provided game id already associated with given source id!");
//                }
//            } else {
//                source.setGames(Set.of(gameId));
//            }
//            db.setValueInMap(ID.ALL_SOURCES, String.valueOf(sourceId), serializationSupport.serialize(source));
//            updateSourceMetadata(source, db);
//            return null;
//        });
//    }
//
//    @Override
//    public void removeEventSourceFromGame(int sourceId, int gameId) {
//        withDbContext(db -> {
//            String valueFromMap = db.getValueFromMap(ID.ALL_SOURCES, String.valueOf(sourceId));
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("No event source is found by given id!");
//            }
//
//            EventSource source = serializationSupport.deserialize(valueFromMap, EventSource.class);
//            if (source.getGames() != null) {
//                if (!source.getGames().remove(gameId)) {
//                    throw new OasisRuntimeException("Provided game id is not associated with given source id!");
//                }
//                db.setValueInMap(ID.ALL_SOURCES, String.valueOf(sourceId), serializationSupport.serialize(source));
//                updateSourceMetadata(source, db);
//            }
//            return null;
//        });
//    }
//
//    @Override
//    public Game addNewGame(Game game) {
//        return withDbContext(db -> {
//            if (existsGame(game.getName())) {
//                throw new OasisDbException("Game by given name '" + game.getName() + "' already exists!");
//            }
//
//            String gameIdStr = String.valueOf(game.getId());
//            db.setValueInMap(ID.ALL_GAMES, gameIdStr, serializationSupport.serialize(game));
//            updateGameIndex(game, null, db);
//            return game;
//        });
//    }
//
//    @Override
//    public Game updateGame(int gameId, Game game) {
//        return withDbContext(db -> {
//            Game existingGame = readGame(gameId);
//            if (gameId != existingGame.getId() || gameId != game.getId()) {
//                throw new OasisRuntimeException("Game id is not allowed to modify!");
//            }
//
//            String prevName = existingGame.getName();
//
//            String gameIdStr = String.valueOf(gameId);
//            db.setValueInMap(ID.ALL_GAMES, gameIdStr, serializationSupport.serialize(game));
//            updateGameIndex(game, prevName, db);
//            return game;
//        });
//    }
//
//    @Override
//    public Game updateGameStatus(int gameId, String status, long updatedAt) {
//        Game game = readGame(gameId);
//        game.setUpdatedAt(updatedAt);
//
//        return updateGame(gameId, game);
//    }
//
//    @Override
//    public Game readGame(int gameId) {
//        return withDbContext(db -> {
//            String gameStr = db.getValueFromMap(ID.ALL_GAMES, String.valueOf(gameId));
//            if (Texts.isEmpty(gameStr)) {
//                throw new OasisRuntimeException("No game is found by id " + gameId + "!");
//            }
//            return serializationSupport.deserialize(gameStr, Game.class);
//        });
//    }
//
//    @Override
//    public Game deleteGame(int gameId) {
//        return withDbContext(db -> {
//            Mapped gamesMap = db.MAP(ID.ALL_GAMES);
//            String subKey = String.valueOf(gameId);
//            String gameStr = gamesMap.getValue(subKey);
//            if (Texts.isEmpty(gameStr)) {
//                throw new OasisDbException("No game exist by given id " + gameId + "!");
//            }
//
//            gamesMap.remove(subKey);
//            Game deletedGame = serializationSupport.deserialize(gameStr, Game.class);
//            db.removeKeyFromMap(ID.ALL_GAMES_INDEX, deletedGame.getName());
//            return deletedGame;
//        });
//    }
//
//    @Override
//    public boolean existsGame(String gameName) {
//        return withDbContext(db -> db.mapKeyExists(ID.ALL_GAMES_INDEX, gameName));
//    }
//
//    @Override
//    public Game readGameByName(String gameName) {
//        return withDbContext(db -> {
//            String gameStr = db.getValueFromMap(ID.ALL_GAMES_INDEX, gameName);
//            if (Texts.isEmpty(gameStr)) {
//                throw new OasisRuntimeException("No game is found by name " + gameName + "!");
//            }
//            return serializationSupport.deserialize(gameStr, Game.class);
//        });
//    }
//
//    @Override
//    public PaginatedResult<Game> listGames(String offset, int pageSize) {
//        return withDbContext(db -> {
//            List<Game> games = new ArrayList<>();
//            Mapped gamesMap = db.MAP(ID.ALL_GAMES);
//            String cur = Texts.isEmpty(offset) ? null : offset;
//            PaginatedResult<Pair<String, String>> searchResult;
//            do {
//                searchResult = gamesMap.search(ALL_PATTERN, pageSize, cur);
//                games.addAll(searchResult.getRecords().stream()
//                        .map(rec -> serializationSupport.deserialize(rec.getRight(), Game.class))
//                        .collect(Collectors.toList()));
//
//            } while (!searchResult.isCompleted());
//            return new PaginatedResult<>(
//                    Texts.isEmpty(searchResult.getNextCursor()) ? null : searchResult.getNextCursor(),
//                    games);
//        });
//    }
//
//    @Override
//    public GameStatus readCurrentGameStatus(int gameId) {
//        throw new OasisRuntimeException("Redis does not support getting current status of game!");
//    }
//
//    @Override
//    public List<GameStatus> readGameStatusHistory(int gameId, long startFrom, long endTo) {
//        throw new OasisRuntimeException("Redis does not support getting history of status of a game!");
//    }
//
//    public Map<String, UserMetadata> readUsersByIdStrings(Collection<String> userIds) {
//        return withDbContext(db -> {
//            if (Utils.isEmpty(userIds)) {
//                return new HashMap<>();
//            }
//
//            String[] keys = userIds.toArray(String[]::new);
//            List<String> valuesFromMap = db.getValuesFromMap(ID.ALL_USERS_NAMES, keys);
//            Map<String, UserMetadata> userMap = new HashMap<>();
//            for (int i = 0; i < keys.length; i++) {
//                String userId = keys[i];
//                if (Texts.isNotEmpty(valuesFromMap.get(i))) {
//                    userMap.put(userId, createUserFromValue(Numbers.asLong(userId), valuesFromMap.get(i)));
//                }
//            }
//            return userMap;
//        });
//    }
//
//    public Map<Long, UserMetadata> readUsersByIds(Collection<Long> userIds) {
//        return withDbContext(db -> {
//            String[] keys = userIds.stream().map(String::valueOf).toArray(String[]::new);
//            List<String> valuesFromMap = db.getValuesFromMap(ID.ALL_USERS_NAMES, keys);
//            Map<Long, UserMetadata> userMap = new HashMap<>();
//            for (int i = 0; i < keys.length; i++) {
//                long userId = Numbers.asLong(keys[i]);
//                userMap.put(userId, createUserFromValue(userId, valuesFromMap.get(i)));
//            }
//            return userMap;
//        });
//    }
//
//    public UserMetadata readUserMetadata(String userId) {
//        return withDbContext(db -> {
//            String valuesFromMap = db.getValueFromMap(ID.ALL_USERS_NAMES, userId);
//            if (StringUtils.isEmpty(valuesFromMap)) {
//                throw new OasisRuntimeException("No user metadata found for given user id! [" + userId + "]");
//            }
//            return createUserFromValue(Long.parseLong(userId), valuesFromMap);
//        });
//    }
//
//    @Override
//    public PlayerObject readPlayer(long userId) {
//        return withDbContext(db -> {
//            String userStr = db.getValueFromMap(ID.ALL_USERS, String.valueOf(userId));
//            if (Texts.isEmpty(userStr)) {
//                throw new OasisRuntimeException("No user found by given id!");
//            }
//            return serializationSupport.deserialize(userStr, PlayerObject.class);
//        });
//    }
//
//    @Override
//    public PlayerObject readPlayer(String email) {
//        return withDbContext(db -> {
//            String userIdStr = db.getValueFromMap(ID.ALL_USERS_INDEX, email);
//            if (Texts.isEmpty(userIdStr)) {
//                throw new OasisRuntimeException("No user found by given email!");
//            }
//            String userStr = db.getValueFromMap(ID.ALL_USERS, userIdStr);
//            return serializationSupport.deserialize(userStr, PlayerObject.class);
//        });
//    }
//
//    public Map<String, TeamMetadata> readTeamsByIdStrings(Collection<String> teamIds) {
//        return withDbContext(db -> {
//            String[] keys = teamIds.stream().map(String::valueOf).toArray(String[]::new);
//            List<String> valuesFromMap = db.getValuesFromMap(ID.ALL_TEAMS_NAMES, keys);
//            Map<String, TeamMetadata> teamMap = new HashMap<>();
//            for (int i = 0; i < keys.length; i++) {
//                int teamId = Numbers.asInt(keys[i]);
//                teamMap.put(keys[i], createTeamFromValue(teamId, valuesFromMap.get(i)));
//            }
//            return teamMap;
//        });
//    }
//
//    public Map<Integer, TeamMetadata> readTeamsById(Collection<Integer> teamIds) {
//        return withDbContext(db -> {
//            String[] keys = teamIds.stream().map(String::valueOf).toArray(String[]::new);
//            List<String> valuesFromMap = db.getValuesFromMap(ID.ALL_TEAMS_NAMES, keys);
//            Map<Integer, TeamMetadata> teamMap = new HashMap<>();
//            for (int i = 0; i < keys.length; i++) {
//                int userId = Numbers.asInt(keys[i]);
//                teamMap.put(userId, createTeamFromValue(userId, valuesFromMap.get(i)));
//            }
//            return teamMap;
//        });
//    }
//
//    public TeamMetadata readTeamMetadata(String teamId) {
//        return withDbContext(db -> {
//            String valuesFromMap = db.getValueFromMap(ID.ALL_TEAMS_NAMES, teamId);
//            if (Texts.isEmpty(valuesFromMap)) {
//                return null;
//            }
//
//            return createTeamFromValue(Integer.parseInt(teamId), valuesFromMap);
//        });
//    }
//
//    @Override
//    public PlayerObject addPlayer(PlayerObject newUser) {
//        return withDbContext(db -> {
//            if (db.mapKeyExists(ID.ALL_USERS_INDEX, newUser.getEmail())) {
//                throw new OasisRuntimeException("User by email already exist!");
//            }
//
//            String userIdStr = String.valueOf(newUser.getId());
//            newUser.setId(newUser.getId());
//            db.setValueInMap(ID.ALL_USERS, userIdStr, serializationSupport.serialize(newUser));
//            updateUserMetadata(newUser, db);
//            updateUserIndex(newUser, db);
//            return newUser;
//        });
//    }
//
//    @Override
//    public boolean existsPlayer(String email) {
//        return withDbContext(db -> db.mapKeyExists(ID.ALL_USERS_INDEX, email));
//    }
//
//    @Override
//    public boolean existsPlayer(long userId) {
//        return withDbContext(db -> existUser(userId, db));
//    }
//
//    @Override
//    public PlayerObject updatePlayer(long userId, PlayerObject updatedUser) {
//        return withDbContext(db -> {
//            String userIdStr = String.valueOf(userId);
//            String userIdOfEmail = db.getValueFromMap(ID.ALL_USERS_INDEX, updatedUser.getEmail());
//            if (Texts.isEmpty(userIdOfEmail) || !userIdStr.equals(userIdOfEmail)) {
//                throw new OasisRuntimeException("No such existing user found by email or id!");
//            }
//
//            updatedUser.setId(userId);
//            db.setValueInMap(ID.ALL_USERS, userIdStr, serializationSupport.serialize(updatedUser));
//            updateUserMetadata(updatedUser, db);
//            return updatedUser;
//        });
//    }
//
//    @Override
//    public PlayerObject deletePlayer(long userId) {
//        return withDbContext(db -> {
//            String userIdStr = String.valueOf(userId);
//            String userVal = db.getValueFromMap(ID.ALL_USERS, userIdStr);
//
//            if (Texts.isEmpty(userVal)) {
//                throw new OasisRuntimeException("No user exist by given id!");
//            }
//
//            db.removeKeyFromMap(ID.ALL_USERS_NAMES, userIdStr);
//            db.removeKeyFromMap(ID.ALL_USERS, userIdStr);
//            PlayerObject playerObject = serializationSupport.deserialize(userVal, PlayerObject.class);
//            db.removeKeyFromMap(ID.ALL_USERS_INDEX, playerObject.getEmail());
//            return playerObject;
//        });
//    }
//
//    @Override
//    public TeamObject addTeam(TeamObject teamObject) {
//        return withDbContext(db -> {
//            if (existsTeam(teamObject.getName())) {
//                throw new OasisRuntimeException("A team is already exist by given name!");
//            }
//
//            String teamIdStr = String.valueOf(teamObject.getId());
//            teamObject.setId(teamObject.getId());
//            db.setValueInMap(ID.ALL_TEAMS, teamIdStr, serializationSupport.serialize(teamObject));
//            updateTeamMetadata(teamObject, db);
//            updateTeamIndex(teamObject, null, db);
//            return teamObject;
//        });
//    }
//
//    @Override
//    public TeamObject readTeam(int teamId) {
//        return withDbContext(db -> {
//            String value = db.getValueFromMap(ID.ALL_TEAMS, String.valueOf(teamId));
//            if (Texts.isEmpty(value)) {
//                throw new OasisRuntimeException("No team is found by given team id!");
//            }
//            return serializationSupport.deserialize(value, TeamObject.class);
//        });
//    }
//
//    @Override
//    public TeamObject readTeam(String teamName) {
//        return withDbContext(db -> {
//            String value = db.getValueFromMap(ID.ALL_TEAMS_INDEX, teamName);
//            if (Texts.isEmpty(value)) {
//                throw new OasisRuntimeException("No team is found by given team id!");
//            }
//            return serializationSupport.deserialize(value, TeamObject.class);
//        });
//    }
//
//    @Override
//    public TeamObject updateTeam(int teamId, TeamObject updatedTeam) {
//        return withDbContext(db -> {
//            String value = db.getValueFromMap(ID.ALL_TEAMS, String.valueOf(teamId));
//            String prevName = null;
//            if (Texts.isNotEmpty(value)) {
//                TeamObject readTeam = serializationSupport.deserialize(value, TeamObject.class);
//                prevName = readTeam.getName();
//            }
//
//            db.setValueInMap(ID.ALL_TEAMS, String.valueOf(teamId), serializationSupport.serialize(updatedTeam));
//            updateTeamMetadata(updatedTeam, db);
//            updateTeamIndex(updatedTeam, prevName, db);
//            return updatedTeam;
//        });
//    }
//
//    @Override
//    public boolean existsTeam(String teamName) {
//        return withDbContext(db -> db.mapKeyExists(ID.ALL_TEAMS_INDEX, teamName.toLowerCase()));
//    }
//
//    @Override
//    public boolean existsTeam(int teamId) {
//        return withDbContext(db -> existTeam(teamId, db));
//    }
//
//    @Override
//    public PaginatedResult<TeamMetadata> searchTeam(String teamName, String cursor, int maxRecords) {
//        return withDbContext(db -> {
//            String searchPattern = "*" + teamName + "*";
//            Mapped gamesMap = db.MAP(ID.ALL_TEAMS_INDEX);
//            String cur = Texts.isEmpty(cursor) ? null : cursor;
//            PaginatedResult<Pair<String, String>> searchResult = gamesMap.search(searchPattern, maxRecords, cur);
//
//            List<String> teamIds = searchResult.getRecords().stream()
//                    .map(Pair::getRight)
//                    .collect(Collectors.toList());
//
//            List<TeamMetadata> list = new ArrayList<>(readTeamsByIdStrings(teamIds).values());
//            return new PaginatedResult<>(
//                    Texts.isEmpty(searchResult.getNextCursor()) ? null : searchResult.getNextCursor(),
//                    list);
//        });
//    }
//
//    @Override
//    public void addPlayerToTeam(long userId, int gameId, int teamId) {
//        withDbContext(db -> {
//            String userFullRef = db.getValueFromMap(ID.ALL_USERS, String.valueOf(userId));
//            if (Texts.isEmpty(userFullRef)) {
//                throw new OasisRuntimeException("Provided user id does not exist!");
//            }
//
//            PlayerObject userRef = serializationSupport.deserialize(userFullRef, PlayerObject.class);
//            String currentUserTeams = Texts.orDefault(db.getValueFromMap(ID.ALL_USERS_TEAMS, userRef.getEmail()));
//            String currentTeamUsers = Texts.orDefault(db.getValueFromMap(ID.ALL_TEAMS_USERS, String.valueOf(teamId)));
//
//            UserAssociationInfo associationInfo;
//            if (Texts.isEmpty(currentUserTeams)) {
//                associationInfo = new UserAssociationInfo();
//                associationInfo.setEmail(userRef.getEmail());
//                associationInfo.setId(userId);
//                associationInfo.setGames(Map.of(gameId, UserAssociationInfo.GameAssociation.ofTeam(teamId)));
//            } else {
//                associationInfo = serializationSupport.deserialize(currentUserTeams, UserAssociationInfo.class);
//                UserAssociationInfo.GameAssociation currAssociation = associationInfo.getGames().get(gameId);
//                if (Objects.isNull(currAssociation)) {
//                    associationInfo.getGames().put(gameId, UserAssociationInfo.GameAssociation.ofTeam(teamId));
//                } else {
//                    throw new OasisRuntimeException("There is already an associated team for the provided game!");
//                }
//            }
//            db.setValueInMap(ID.ALL_USERS_TEAMS, userRef.getEmail(), serializationSupport.serialize(associationInfo));
//
//            Set<String> teamUserSet = splitToSet(currentTeamUsers);
//            if (teamUserSet.add(String.valueOf(userId))) {
//                db.setValueInMap(ID.ALL_TEAMS_USERS, String.valueOf(teamId), String.join(COMMA, teamUserSet));
//            }
//            return null;
//        });
//    }
//
//    @Override
//    public void removePlayerFromTeam(long userId, int gameId, int teamId) {
//        withDbContext(db -> {
//            String userFullRef = db.getValueFromMap(ID.ALL_USERS, String.valueOf(userId));
//            if (Texts.isEmpty(userFullRef)) {
//                throw new OasisRuntimeException("Provided user id does not exist!");
//            } else if (!existTeam(teamId, db)) {
//                throw new OasisRuntimeException("Provided team id does not exist!");
//            }
//
//            PlayerObject userRef = serializationSupport.deserialize(userFullRef, PlayerObject.class);
//            String currentUserTeams = Texts.orDefault(db.getValueFromMap(ID.ALL_USERS_TEAMS, userRef.getEmail()));
//            String currentTeamUsers = Texts.orDefault(db.getValueFromMap(ID.ALL_TEAMS_USERS, String.valueOf(teamId)));
//
//            if (!Texts.isEmpty(currentUserTeams)) {
//                UserAssociationInfo associationInfo = serializationSupport.deserialize(currentUserTeams, UserAssociationInfo.class);
//                if (associationInfo.getGames().remove(gameId, UserAssociationInfo.GameAssociation.ofTeam(teamId))) {
//                    db.setValueInMap(ID.ALL_USERS_TEAMS, userRef.getEmail(), serializationSupport.serialize(associationInfo));
//                } else {
//                    throw new OasisRuntimeException("Given team id was not associated with user id!");
//                }
//            }
//
//            Set<String> teamUserSet = splitToSet(currentTeamUsers);
//            if (teamUserSet.remove(String.valueOf(userId))) {
//                db.setValueInMap(ID.ALL_TEAMS_USERS, String.valueOf(teamId), String.join(COMMA, teamUserSet));
//            }
//            return null;
//        });
//    }
//
//    @Override
//    public List<TeamObject> getPlayerTeams(long userId) {
//        return withDbContext(db -> {
//            String userFullRef = db.getValueFromMap(ID.ALL_USERS, String.valueOf(userId));
//            if (Texts.isEmpty(userFullRef)) {
//                throw new OasisRuntimeException("No user is found by given id!");
//            }
//
//            PlayerObject userRef = serializationSupport.deserialize(userFullRef, PlayerObject.class);
//            String currentUserTeams = Texts.orDefault(db.getValueFromMap(ID.ALL_USERS_TEAMS, userRef.getEmail()));
//            if (Texts.isEmpty(currentUserTeams)) {
//                return new ArrayList<>();
//            }
//
//            UserAssociationInfo userAssociationInfo = serializationSupport.deserialize(currentUserTeams, UserAssociationInfo.class);
//            return userAssociationInfo.getGames().values().stream().map(userTeam -> {
//                String teamJson = db.getValueFromMap(ID.ALL_TEAMS, String.valueOf(userTeam.getTeam()));
//                return serializationSupport.deserialize(teamJson, TeamObject.class);
//            }).collect(Collectors.toList());
//        });
//    }
//
//    @Override
//    public List<PlayerObject> getTeamPlayers(int teamId) {
//        return withDbContext(db -> {
//            if (!existTeam(teamId, db)) {
//                throw new OasisRuntimeException("No team is found by given id!");
//            }
//
//            String currentTeamUsers = Texts.orDefault(db.getValueFromMap(ID.ALL_TEAMS_USERS, String.valueOf(teamId)));
//            List<String> teamUserList = Stream.of(currentTeamUsers.split(COMMA)).filter(Texts::isNotEmpty).collect(Collectors.toList());
//            return teamUserList.stream().map(userTeam -> {
//                String userJson = db.getValueFromMap(ID.ALL_USERS, userTeam);
//                return serializationSupport.deserialize(userJson, PlayerObject.class);
//            }).collect(Collectors.toList());
//        });
//    }
//
//    @Override
//    public ElementDef addNewElement(int gameId, ElementDef elementDef) {
//        return withDbContext(db -> {
//            String id = elementDef.getElementId();
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//            elementDef.setGameId(gameId);
//
//            if (db.mapKeyExists(baseKey, id)) {
//                throw new OasisRuntimeException("Element by given id already exists!");
//            }
//
//            db.setValueInMap(baseKey, id, serializationSupport.serialize(elementDef));
//            updateElementMetadata(elementDef, db);
//            setToElementByType(elementDef, db);
//            return elementDef;
//        });
//    }
//
//    @Override
//    public ElementDef updateElement(int gameId, String id, SimpleElementDefinition elementDef) {
//        return withDbContext(db -> {
//            if (!id.equals(elementDef.getId())) {
//                throw new OasisRuntimeException("Provided id and element id mismatches!");
//            }
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//
//            if (!db.mapKeyExists(baseKey, id)) {
//                throw new OasisRuntimeException("Element by given id does not exist!");
//            }
//
//            ElementDef dbElement = readElement(gameId, id);
//            dbElement.setMetadata(elementDef);
//            db.setValueInMap(baseKey, id, serializationSupport.serialize(dbElement));
//            updateElementMetadata(elementDef, gameId, db);
//            setToElementByType(dbElement, db);
//            return dbElement;
//        });
//    }
//
//    @Override
//    public ElementDef deleteElement(int gameId, String id) {
//        return withDbContext(db -> {
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//
//            String valueFromMap = db.getValueFromMap(baseKey, id);
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("Element by given id does not exist!");
//            }
//
//            db.removeKeyFromMap(baseKey, id);
//            db.removeKeyFromMap(ID.getBasicElementDefKeyForGame(gameId), id);
//            ElementDef elementDef = serializationSupport.deserialize(valueFromMap, ElementDef.class);
//            db.removeKeyFromMap(ID.getElementMetadataByTypeForGame(gameId, elementDef.getType()), elementDef.getElementId());
//            return elementDef;
//        });
//    }
//
//    @Override
//    public ElementDef readElement(int gameId, String id) {
//        return withDbContext(db -> {
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//
//            String valueFromMap = db.getValueFromMap(baseKey, id);
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("Element by given id does not exist!");
//            }
//
//            return serializationSupport.deserialize(valueFromMap, ElementDef.class);
//        });
//    }
//
//    @Override
//    public ElementDef readElementWithoutData(int gameId, String id) {
//        return withDbContext(db -> {
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//
//            String valueFromMap = db.getValueFromMap(baseKey, id);
//            if (Texts.isEmpty(valueFromMap)) {
//                throw new OasisRuntimeException("Element by given id does not exist!");
//            }
//
//            ElementDef def = serializationSupport.deserialize(valueFromMap, ElementDef.class);
//            def.setData(null);
//            return def;
//        });
//    }
//
//    @Override
//    public List<ElementDef> readElementsByType(int gameId, String type) {
//        return withDbContext(db -> {
//            String baseKey = ID.getElementMetadataByTypeForGame(gameId, type);
//
//            Map<String, String> all = db.MAP(baseKey).getAll();
//            List<ElementDef> elementDefinitions = new ArrayList<>();
//            if (all != null) {
//                List<String> allKeys = new ArrayList<>(all.keySet());
//                String elementDefBaseKey = ID.getBasicElementDefKeyForGame(gameId);
//                List<String> valuesFromMap = db.getValuesFromMap(elementDefBaseKey, allKeys.toArray(new String[0]));
//                for (int i = 0; i < allKeys.size(); i++) {
//                    ElementDef def = serializationSupport.deserialize(valuesFromMap.get(i), ElementDef.class);
//                    if (def != null) {
//                        elementDefinitions.add(def);
//                    }
//                }
//            }
//            return elementDefinitions;
//        });
//    }
//
//    @Override
//    public List<ElementDef> readElementsByGameId(int gameId) {
//        return withDbContext(db -> {
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//
//            Mapped elementsMap = db.MAP(baseKey);
//            return elementsMap.getAll()
//                    .values().stream()
//                    .map(content -> serializationSupport.deserialize(content, ElementDef.class))
//                    .peek(def -> def.setData(null))
//                    .collect(Collectors.toList());
//        });
//    }
//
//    public SimpleElementDefinition readElementDefinition(int gameId, String id) {
//        return withDbContext(db -> {
//            String baseKey = ID.getBasicElementDefKeyForGame(gameId);
//            String metadata = db.getValueFromMap(baseKey, id);
//
//            if (Texts.isEmpty(metadata)) {
//                return null;
//            }
//            SimpleElementDefinition def = serializationSupport.deserialize(metadata, SimpleElementDefinition.class);
//            def.setId(id);
//            return def;
//        });
//    }
//
//    public Map<String, SimpleElementDefinition> readElementDefinitions(int gameId, Collection<String> ids) {
//        return withDbContext(db -> {
//            String baseKey = ID.getBasicElementDefKeyForGame(gameId);
//
//            List<String> elementValues = db.getValuesFromMap(baseKey, ids.toArray(new String[0]));
//            Map<String, SimpleElementDefinition> defs = new HashMap<>();
//            for (String value : elementValues) {
//                SimpleElementDefinition def = serializationSupport.deserialize(value, SimpleElementDefinition.class);
//                if (def != null) {
//                    defs.put(def.getId(), def);
//                }
//            }
//            return defs;
//        });
//    }
//
//    public List<SimpleElementDefinition> listAllElementDefinitions(int gameId, String type) {
//        return withDbContext(db -> {
//            String baseKey = ID.getElementMetadataByTypeForGame(gameId, type);
//
//            Map<String, String> all = db.MAP(baseKey).getAll();
//            List<SimpleElementDefinition> elementDefinitions = new ArrayList<>();
//            if (all != null) {
//                for (Map.Entry<String, String> entry : all.entrySet()) {
//                    SimpleElementDefinition def = serializationSupport.deserialize(entry.getValue(), SimpleElementDefinition.class);
//                    elementDefinitions.add(def);
//                }
//            }
//            return elementDefinitions;
//        });
//    }
//
//    public ElementDef readFullElementDef(int gameId, String ruleId) {
//        return withDbContext(db -> {
//            String baseKey = ID.getDetailedElementDefKeyForGame(gameId);
//
//            Mapped elementsMap = db.MAP(baseKey);
//            String elementData = elementsMap.getValue(ruleId);
//            return serializationSupport.deserialize(elementData, ElementDef.class);
//        });
//    }
//
//    @Override
//    public RankInfo addRank(int gameId, RankInfo newRank) {
//        return withDbContext(db -> {
//            String baseKey = ID.getGameRanksInfoKey(gameId);
//            String attrId = String.valueOf(newRank.getId());
//            String ranks = db.getValueFromMap(baseKey, ALL_RANKS_KEY);
//            if (Texts.isEmpty(ranks)) {
//                db.setValueInMap(baseKey, ALL_RANKS_KEY, attrId);
//            } else {
//                Set<String> attrs = splitToSet(ranks);
//                if (!attrs.add(attrId)) {
//                    throw new OasisRuntimeException("Rank already exist by given id!");
//                }
//                db.setValueInMap(baseKey, ALL_RANKS_KEY, String.join(COMMA, attrs));
//            }
//
//            db.setValueInMap(baseKey, attrId, serializationSupport.serialize(newRank));
//            return newRank;
//        });
//    }
//
//    @Override
//    public List<RankInfo> listAllRanks(int gameId) {
//        Map<Integer, RankInfo> rankInfoMap = withDbContext(db -> {
//            String baseKey = ID.getGameRanksInfoKey(gameId);
//            String ranks = db.getValueFromMap(baseKey, ALL_RANKS_KEY);
//            if (Texts.isEmpty(ranks)) {
//                return new HashMap<>();
//            }
//
//            List<String> elementValues = db.getValuesFromMap(baseKey, Stream.of(ranks.split(COMMA))
//                    .distinct().toArray(String[]::new));
//            Map<Integer, RankInfo> defs = new HashMap<>();
//            for (String elementValue : elementValues) {
//                RankInfo rankInfo = serializationSupport.deserialize(elementValue, RankInfo.class);
//                defs.put(rankInfo.getId(), rankInfo);
//            }
//            return defs;
//        });
//        return new ArrayList<>(rankInfoMap.values());
//    }
//
//    ////////////////////////////////////////////////////////////////////////////////////////
//    //
//    //  Private Methods
//    //
//    ///////////////////////////////////////////////////////////////////////////////////////
//
//    private Set<String> splitToSet(String text) {
//        return Stream.of(text.split(COMMA)).filter(Texts::isNotEmpty).collect(Collectors.toSet());
//    }
//
//    private boolean existEventSource(String token, DbContext db) {
//        return db.mapKeyExists(ID.ALL_SOURCES_INDEX, token);
//    }
//
//    private boolean existTeam(int teamId, DbContext db) {
//        return db.mapKeyExists(ID.ALL_TEAMS, String.valueOf(teamId));
//    }
//
//    private boolean existUser(long userId, DbContext db) {
//        return db.mapKeyExists(ID.ALL_USERS, String.valueOf(userId));
//    }
//
//    private UserMetadata createUserFromValue(long id, String val) {
//        if (StringUtils.isEmpty(val)) {
//            return null;
//        }
//        UserMetadata userMetadata = serializationSupport.deserialize(val, UserMetadata.class);
//        userMetadata.setUserId(id);
//        return userMetadata;
//    }
//
//    private TeamMetadata createTeamFromValue(int id, String val) {
//        return new TeamMetadata(id, val);
//    }
//
//    private void updateSourceMetadata(EventSource eventSourceOriginal, DbContext db) {
//        EventSourceMetadata source = eventSourceOriginal.createCopyOfMeta();
//        db.setValueInMap(ID.ALL_SOURCES_INDEX, source.getToken(), serializationSupport.serialize(source));
//    }
//
//    private void updateTeamMetadata(TeamObject teamObject, DbContext db) {
//        TeamMetadata teamMetadata = new TeamMetadata(teamObject.getId(), teamObject.getName());
//        db.setValueInMap(ID.ALL_TEAMS_NAMES, String.valueOf(teamMetadata.getTeamId()), teamMetadata.getName());
//    }
//
//    private void updateUserIndex(PlayerObject playerObject, DbContext db) {
//        db.setValueInMap(ID.ALL_USERS_INDEX, playerObject.getEmail(), String.valueOf(playerObject.getId()));
//    }
//
//    private void updateTeamIndex(TeamObject teamObject, String prevName, DbContext db) {
//        if (!teamObject.getName().equals(prevName)) {
//            if (prevName != null) {
//                db.removeKeyFromMap(ID.ALL_TEAMS_INDEX, prevName.toLowerCase());
//            }
//            db.setValueInMap(ID.ALL_TEAMS_INDEX, teamObject.getName().toLowerCase(), String.valueOf(teamObject.getId()));
//        }
//    }
//
//    private void updateGameIndex(Game game, String prevName, DbContext db) {
//        if (!game.getName().equals(prevName)) {
//            if (prevName != null) {
//                db.removeKeyFromMap(ID.ALL_GAMES_INDEX, prevName);
//            }
//            db.setValueInMap(ID.ALL_GAMES_INDEX, game.getName(), String.valueOf(game.getId()));
//        }
//    }
//
//    private void updateUserMetadata(PlayerObject playerObject, DbContext db) {
//        UserMetadata metadata = new UserMetadata(playerObject.getId(),
//                playerObject.getDisplayName(),
//                playerObject.getTimeZone(),
//                playerObject.getGender() != null ? playerObject.getGender().name() : null);
//        db.setValueInMap(ID.ALL_USERS_NAMES, String.valueOf(playerObject.getId()), serializationSupport.serialize(metadata));
//    }
//
//    private void updateElementMetadata(ElementDef def, DbContext db) {
//        SimpleElementDefinition simpleElementDefinition = def.getMetadata();
//        updateElementMetadata(simpleElementDefinition, def.getGameId(), db);
//    }
//
//    private void updateElementMetadata(SimpleElementDefinition simpleElementDefinition, int gameId, DbContext db) {
//        db.setValueInMap(ID.getBasicElementDefKeyForGame(gameId),
//                simpleElementDefinition.getId(), serializationSupport.serialize(simpleElementDefinition));
//    }
//
//    private void setToElementByType(ElementDef def, DbContext db) {
//        SimpleElementDefinition metadata = def.getMetadata();
//        db.setValueInMap(ID.getElementMetadataByTypeForGame(def.getGameId(), def.getType()),
//                def.getElementId(),
//                serializationSupport.serialize(metadata));
//    }
//
//    private <T> T withDbContext(Handler<T> executor) {
//        try (DbContext dbContext = dbPool.createContext()) {
//            return executor.execute(dbContext);
//        } catch (IOException | OasisException e) {
//            throw new OasisRuntimeException(e.getMessage(), e);
//        }
//    }
//
//    interface Handler<T> {
//
//        T execute(DbContext db) throws OasisException;
//
//    }
}
