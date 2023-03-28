package com.dimpt.onp.module.simulation.bs.service.impl;

import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.dimpt.onp.common.bean.em.YesAndNoEnum;
import com.dimpt.onp.common.bean.exception.BizException;
import com.dimpt.onp.common.utils.BigDecimalMathUtils;
import com.dimpt.onp.function.mybatis.Tx;
import com.dimpt.onp.function.security.AuthenticationUtils;
import com.dimpt.onp.module.basic.domain.dto.NotificationDTO;
import com.dimpt.onp.module.basic.domain.em.NotificationConsumerTypeEnum;
import com.dimpt.onp.module.basic.domain.em.NotificationTypeEnum;
import com.dimpt.onp.module.basic.service.NotificationService;
import com.dimpt.onp.module.equipment.service.OsnrFormulaService;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsBusinessDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsBusinessGroupDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsBusinessRouteDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsBusinessSimulationLogDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsBusinessSimulationResultDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsBusinessSimulationStrategyDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsLinkDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsLocalDimensionChannelDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsLocalDimensionDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsNodeDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsOmsChannelDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsOmsDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsProjectDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsRoutePolicyDO;
import com.dimpt.onp.module.simulation.bs.dao.entity.BsSrlgDO;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsBusinessGroupMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsBusinessMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsBusinessRouteMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsBusinessSimulationLogMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsBusinessSimulationResultMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsBusinessSimulationStrategyMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsLinkMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsLocalDimensionChannelMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsLocalDimensionMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsNodeMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsOmsChannelMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsOmsMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsProjectMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsRoutePolicyMapper;
import com.dimpt.onp.module.simulation.bs.dao.mapper.BsSrlgMapper;
import com.dimpt.onp.module.simulation.bs.domain.condition.BsLinkConditions;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsBusinessDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsBusinessRelayOverviewDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsBusinessRouteDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsBusinessRouteDTO.RouteNode;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsBusinessSimulationStrategyDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsCrossDomainRelationshipDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsLinkDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsProjectConfigurationDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsProjectDTO;
import com.dimpt.onp.module.simulation.bs.domain.dto.BsProjectSimulationStrategyDTO;
import com.dimpt.onp.module.simulation.bs.domain.em.BsDistributionTypeEnum;
import com.dimpt.onp.module.simulation.bs.domain.em.BsProjectStageEnum;
import com.dimpt.onp.module.simulation.bs.domain.em.BusinessSimulationStatusEnum;
import com.dimpt.onp.module.simulation.bs.domain.em.NetworkSourceTypeEnum;
import com.dimpt.onp.module.simulation.bs.domain.em.RoutePolicyEnum;
import com.dimpt.onp.module.simulation.bs.domain.em.RouteSeparationPolicyEnum;
import com.dimpt.onp.module.simulation.bs.service.BsBusinessService;
import com.dimpt.onp.module.simulation.bs.service.BsBusinessSimulationService;
import com.dimpt.onp.module.simulation.bs.service.BsLinkService;
import com.dimpt.onp.module.simulation.bs.service.BsProjectAuthorizationService;
import com.dimpt.onp.module.simulation.bs.service.BsProjectConfigurationService;
import com.dimpt.onp.module.simulation.bs.service.BsProjectService;
import com.dimpt.onp.module.simulation.bs.service.BsSystemService;
import com.dimpt.onp.module.simulation.bs.utils.BsBusinessRouteUtils;
import com.dimpt.onp.module.simulation.bs.utils.DataStandardDeviationUtil;
import com.dimpt.onp.module.simulation.common.OsnrThresholdDefinition;
import com.dimpt.onp.module.simulation.common.algo.CombinationUtils;
import com.dimpt.onp.module.simulation.common.algo.path.KShortestPath;
import com.dimpt.onp.module.simulation.common.algo.path.PathUtils;
import com.dimpt.onp.module.simulation.common.domain.em.LinkRoleEnum;
import com.dimpt.onp.module.simulation.common.domain.em.NodeTypeEnum;
import com.dimpt.onp.module.simulation.common.domain.em.OmsProtectionMechanismEnum;
import com.dimpt.onp.module.simulation.common.exception.ProjectFrozenException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Sets;
import org.apache.commons.lang3.StringUtils;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 业务仿真服务层实现
 *
 * @author mawk mawenkai@dimpt.com
 */
@Service
public class BsBusinessSimulationServiceImpl implements BsBusinessSimulationService {

    private static final Logger log = LoggerFactory.getLogger(BsBusinessSimulationServiceImpl.class);

    private static final String TASK_MANAGER_NOTIFICATION_BIZ_CODE = "BS_BUSINESS_SIMULATION";
    /**
     * 业务复用段超长标识，单位：m
     */
    private static final long BMS_OVER_LENGTH_LIMIT = 800000;

    /**
     * 跨域连接权重
     */
    private static final long CROSS_DOMAIN_RELATIONSHIP_WEIGHT = 500000;

    /**
     * 业务仿真最大并行数量
     */
    private static final int MAXIMUM_PARALLEL_NUMBER_OF_BUSINESS_SIMULATIONS = 10;

    /**
     * 执行业务仿真的线程池
     */
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            5, MAXIMUM_PARALLEL_NUMBER_OF_BUSINESS_SIMULATIONS, 10, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

    private final ObjectMapper objectMapper;

    private final BsProjectService bsProjectService;

    private final BsSystemService bsSystemService;

    private final OsnrFormulaService osnrFormulaService;

    private final BsBusinessService bsBusinessService;

    private final BsLinkService bsLinkService;

    private final NotificationService notificationService;

    private final BsProjectConfigurationService bsProjectConfigurationService;

    private final BsProjectAuthorizationService bsProjectAuthorizationService;

    private final BsProjectMapper bsProjectMapper;

    private final BsBusinessMapper bsBusinessMapper;

    private final BsNodeMapper bsNodeMapper;

    private final BsOmsMapper bsOmsMapper;

    private final BsLinkMapper bsLinkMapper;

    private final BsLocalDimensionMapper bsLocalDimensionMapper;

    private final BsLocalDimensionChannelMapper bsLocalDimensionChannelMapper;

    private final BsOmsChannelMapper bsOmsChannelMapper;

    private final BsBusinessGroupMapper bsBusinessGroupMapper;

    private final BsBusinessSimulationResultMapper bsBusinessSimulationResultMapper;

    private final BsBusinessSimulationStrategyMapper businessSimulationStrategyMapper;

    private final BsBusinessSimulationLogMapper bsBusinessSimulationLogMapper;

    private final BsBusinessRouteMapper bsBusinessRouteMapper;

    private final BsRoutePolicyMapper policyMapper;

    private final BsSrlgMapper bsSrlgMapper;

    /**
     * Instantiates a new Bs business simulation service.
     *
     * @param objectMapper                     the object mapper
     * @param bsProjectService                 the bs project service
     * @param bsSystemService                  the bs system service
     * @param osnrFormulaService               the osnr formula service
     * @param bsBusinessService                the bs business service
     * @param bsLinkService                    the bs link service
     * @param notificationService              the notification service
     * @param bsProjectConfigurationService    the bs project configuration service
     * @param bsProjectAuthorizationService    the bs project authorization service
     * @param bsProjectMapper                  the bs project mapper
     * @param bsBusinessMapper                 the bs business mapper
     * @param bsNodeMapper                     the bs node mapper
     * @param bsOmsMapper                      the bs oms mapper
     * @param bsLinkMapper                     the bs link mapper
     * @param bsLocalDimensionMapper           the bs local dimension mapper
     * @param bsLocalDimensionChannelMapper    the bs local dimension channel mapper
     * @param bsOmsChannelMapper               the bs oms channel mapper
     * @param bsBusinessGroupMapper            the bs business group mapper
     * @param bsBusinessSimulationResultMapper the bs business simulation result mapper
     * @param businessSimulationStrategyMapper the business simulation strategy mapper
     * @param bsBusinessSimulationLogMapper    the bs business simulation log mapper
     * @param bsBusinessRouteMapper            the bs business route mapper
     * @param policyMapper                     the policy mapper
     * @param bsSrlgMapper                     the bs srlg mapper
     */
    public BsBusinessSimulationServiceImpl(ObjectMapper objectMapper,
                                           BsProjectService bsProjectService,
                                           BsSystemService bsSystemService,
                                           OsnrFormulaService osnrFormulaService,
                                           BsBusinessService bsBusinessService,
                                           BsLinkService bsLinkService,
                                           NotificationService notificationService,
                                           BsProjectConfigurationService bsProjectConfigurationService,
                                           BsProjectAuthorizationService bsProjectAuthorizationService,
                                           BsProjectMapper bsProjectMapper,
                                           BsBusinessMapper bsBusinessMapper,
                                           BsNodeMapper bsNodeMapper,
                                           BsOmsMapper bsOmsMapper,
                                           BsLinkMapper bsLinkMapper,
                                           BsLocalDimensionMapper bsLocalDimensionMapper,
                                           BsLocalDimensionChannelMapper bsLocalDimensionChannelMapper,
                                           BsOmsChannelMapper bsOmsChannelMapper,
                                           BsBusinessGroupMapper bsBusinessGroupMapper,
                                           BsBusinessSimulationResultMapper bsBusinessSimulationResultMapper,
                                           BsBusinessSimulationStrategyMapper businessSimulationStrategyMapper,
                                           BsBusinessSimulationLogMapper bsBusinessSimulationLogMapper,
                                           BsBusinessRouteMapper bsBusinessRouteMapper,
                                           BsRoutePolicyMapper policyMapper,
                                           BsSrlgMapper bsSrlgMapper) {
        this.objectMapper = objectMapper;
        this.bsProjectService = bsProjectService;
        this.bsSystemService = bsSystemService;
        this.bsProjectAuthorizationService = bsProjectAuthorizationService;
        this.bsProjectMapper = bsProjectMapper;
        this.osnrFormulaService = osnrFormulaService;
        this.bsBusinessService = bsBusinessService;
        this.bsLinkService = bsLinkService;
        this.notificationService = notificationService;
        this.bsProjectConfigurationService = bsProjectConfigurationService;
        this.bsBusinessMapper = bsBusinessMapper;
        this.bsNodeMapper = bsNodeMapper;
        this.bsOmsMapper = bsOmsMapper;
        this.bsLinkMapper = bsLinkMapper;
        this.bsLocalDimensionMapper = bsLocalDimensionMapper;
        this.bsLocalDimensionChannelMapper = bsLocalDimensionChannelMapper;
        this.bsOmsChannelMapper = bsOmsChannelMapper;
        this.bsBusinessGroupMapper = bsBusinessGroupMapper;
        this.bsBusinessSimulationResultMapper = bsBusinessSimulationResultMapper;
        this.businessSimulationStrategyMapper = businessSimulationStrategyMapper;
        this.bsBusinessSimulationLogMapper = bsBusinessSimulationLogMapper;
        this.bsBusinessRouteMapper = bsBusinessRouteMapper;
        this.policyMapper = policyMapper;
        this.bsSrlgMapper = bsSrlgMapper;
    }

    @Override
    public BsBusinessRelayOverviewDTO getBusinessRelayOverview(Long businessId, List<Long> relayNodeIdList) {
        Preconditions.checkNotNull(businessId);

        BsBusinessDTO business = bsBusinessService.getBusinessSimple(businessId);
        if (business == null) {
            throw new BizException("业务不存在");
        }

        if (!BusinessSimulationStatusEnum.FINISH.equals(business.getSimulationStatus())) {
            throw new BizException("业务未完成仿真计算");
        }

        BsBusinessRouteDTO simulationRoute = bsBusinessService.getBusinessSimulationRoute(business.getId());

        BsProjectConfigurationDTO projectConfiguration =
                bsProjectConfigurationService.getProjectConfigurationByProjectId(business.getBsProjectId());

        return this.getBusinessRelayOverview(projectConfiguration.getSimulationStrategy(), simulationRoute, relayNodeIdList);
    }

    @Tx
    @Override
    public void businessSimulation(Long projectId, List<Long> businessIdList) {
        Preconditions.checkNotNull(projectId);

        BsProjectDTO project = bsProjectService.getProjectByProjectId(projectId);

        // 检查当前登陆人是否拥有该项目权限
        bsProjectAuthorizationService.checkCurrentUserProjectAuthorization(projectId);

        if (YesAndNoEnum.YES.equals(project.getFrozen())) {
            throw new ProjectFrozenException();
        }

        if (BsProjectStageEnum.SIMULATION_CALCULATION.equals(project.getStage())) {
            throw new BizException("仿真进行中");
        } else if (BsProjectStageEnum.OPTICAL_LAYER_DESIGN.equals(project.getStage())) {
            throw new BizException("项目阶段错误");
        }

        BsProjectConfigurationDTO projectConfiguration = bsProjectConfigurationService.getProjectConfigurationByProjectId(projectId);
        if (projectConfiguration.getSimulationStrategy() == null) {
            throw new BizException("全局仿真配置未完成");
        }
        if (NetworkSourceTypeEnum.NONE.equals(project.getNetworkSourceType())
                && projectConfiguration.getWavelengthAssignmentScheme() == null) {
            throw new BizException("波道配置未完成");
        }

        if (CollectionUtils.isEmpty(businessIdList)) {
            return;
        }
        Set<Long> businessIdSet = new HashSet<>(businessIdList);
        int update = bsBusinessMapper.update(null, Wrappers.lambdaUpdate(BsBusinessDO.class)
                .set(BsBusinessDO::getSimulationStatus, BusinessSimulationStatusEnum.PROCESSING.getCode())
                .in(BsBusinessDO::getId, businessIdSet));
        if (update != businessIdSet.size()) {
            throw new BizException("业务不存在");
        }

        bsProjectMapper.update(null, Wrappers.lambdaUpdate(BsProjectDO.class)
                .set(BsProjectDO::getStage, BsProjectStageEnum.SIMULATION_CALCULATION.getCode())
                .set(BsProjectDO::getCurrentEditorId, AuthenticationUtils.getCurrentThreadUserOrException().getUserId())
                .eq(BsProjectDO::getId, projectId));
        bsProjectService.updateProjectLastModificationUser(projectId);

        try {
            EXECUTOR_SERVICE.submit(() -> {
                try {
                    this.doBatchBusinessSimulation(projectId, projectConfiguration, businessIdList);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        } catch (RejectedExecutionException e) {
            throw new BizException("仿真中的项目数量达到最大值");
        }
    }

    @Tx
    @Override
    public void businessSimulationClear(Long projectId, List<Long> businessIdList) {
        Preconditions.checkNotNull(projectId);

        BsProjectDO project = bsProjectMapper.selectById(projectId);
        if (project == null) {
            throw new BizException("项目不存在");
        }

        // 检查当前登陆人是否拥有该项目权限
        bsProjectAuthorizationService.checkCurrentUserProjectAuthorization(projectId);

        if (YesAndNoEnum.YES.getCode() == project.getFrozen()) {
            throw new ProjectFrozenException();
        }

        if (BsProjectStageEnum.SIMULATION_CALCULATION.getCode() == project.getStage()) {
            throw new BizException("仿真进行中");
        }

        if (CollectionUtils.isEmpty(businessIdList)) {
            return;
        }

        bsBusinessService.clearBusinessSimulation(businessIdList);
        bsBusinessMapper.update(null, Wrappers.lambdaUpdate(BsBusinessDO.class)
                .set(BsBusinessDO::getSimulationStatus, BusinessSimulationStatusEnum.NONE.getCode())
                .in(BsBusinessDO::getId, businessIdList));
        bsProjectService.updateProjectStage(projectId, null);
    }

    /**
     * On application ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // 启动容器时，加载数据库中处于“仿真计算”阶段的项目
        List<BsProjectDO> projectList = bsProjectMapper.selectList(Wrappers.lambdaQuery(BsProjectDO.class)
                .eq(BsProjectDO::getStage, BsProjectStageEnum.SIMULATION_CALCULATION));
        if (projectList.isEmpty()) {
            return;
        }
        List<Long> projectIdList = projectList.stream().map(BsProjectDO::getId).collect(Collectors.toList());

        List<BsBusinessDO> businessList = bsBusinessMapper.selectList(Wrappers.lambdaQuery(BsBusinessDO.class)
                .in(BsBusinessDO::getBsProjectId, projectIdList)
                .eq(BsBusinessDO::getSimulationStatus, BusinessSimulationStatusEnum.PROCESSING.getCode()));
        Map<Long, List<BsBusinessDO>> projectIdBusinessListMap = businessList.stream()
                .collect(Collectors.groupingBy(BsBusinessDO::getBsProjectId));

        try {
            for (Long projectId : projectIdList) {
                List<Long> projectBusinessIdList = projectIdBusinessListMap
                        .getOrDefault(projectId, Collections.emptyList())
                        .stream()
                        .map(BsBusinessDO::getId)
                        .collect(Collectors.toList());

                BsProjectConfigurationDTO projectConfiguration = bsProjectConfigurationService.getProjectConfigurationByProjectId(projectId);
                EXECUTOR_SERVICE.submit(() -> {
                    try {
                        this.doBatchBusinessSimulation(projectId, projectConfiguration, projectBusinessIdList);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }
        } catch (RejectedExecutionException e) {
            throw new BizException("仿真中的项目数量达到最大值");
        }
    }

    /**
     * 获取业务中继方案性能概览
     *
     * @param projectSimulationStrategy 项目仿真策略
     * @param route                     业务路由
     * @param relayNodeIdList           中继方案
     * @return 业务中继方案性能概览
     */
    private BsBusinessRelayOverviewDTO getBusinessRelayOverview(BsProjectSimulationStrategyDTO projectSimulationStrategy,
                                                                BsBusinessRouteDTO route,
                                                                List<Long> relayNodeIdList) {
        List<BsBusinessRouteDTO.RouteBms> bmsList = BsBusinessRouteUtils.getBmsList(route, Optional.ofNullable(relayNodeIdList)
                .orElse(Collections.emptyList()));

        BsBusinessRelayOverviewDTO overview = new BsBusinessRelayOverviewDTO();
        overview.setRelayNodeIdList(relayNodeIdList);
        overview.setItems(this.getFiberBreakingOverview(bmsList, projectSimulationStrategy));

        overview.setSatisfied(YesAndNoEnum.YES);
        for (BsBusinessRelayOverviewDTO.Item overviewItem : overview.getItems()) {
            for (BsBusinessRelayOverviewDTO.BmsOverview bmsOverview : overviewItem.getBmsList()) {
                if (bmsOverview.getOsnrValue().compareTo(bmsOverview.getThresholdValue()) < 0) {
                    overview.setSatisfied(YesAndNoEnum.NO);
                    break;
                }
            }
            if (!YesAndNoEnum.YES.equals(overview.getSatisfied())) {
                break;
            }
        }

        int activeOverlengthCount = 0, standbyOverlengthCount = 0;
        for (BsBusinessRouteDTO.RouteBms routeBms : bmsList) {
            long activeOverlength = 0, standbyOverlength = 0;
            for (BsBusinessRouteDTO.RouteOms routeOms : routeBms.getOmsList()) {
                if (OmsProtectionMechanismEnum.NONE.equals(routeOms.getProtectionMechanism())) {
                    if (routeOms.getLinkMap().containsKey(LinkRoleEnum.ACTIVE)) {
                        activeOverlength += routeOms.getLinkMap().get(LinkRoleEnum.ACTIVE).getDistance();
                        standbyOverlength += routeOms.getLinkMap().get(LinkRoleEnum.ACTIVE).getDistance();
                    }
                } else if (OmsProtectionMechanismEnum.OMSP.equals(routeOms.getProtectionMechanism())) {
                    if (routeOms.getLinkMap().containsKey(LinkRoleEnum.ACTIVE)) {
                        activeOverlength += routeOms.getLinkMap().get(LinkRoleEnum.ACTIVE).getDistance();
                    }
                    if (routeOms.getLinkMap().containsKey(LinkRoleEnum.STANDBY)) {
                        standbyOverlength += routeOms.getLinkMap().get(LinkRoleEnum.STANDBY).getDistance();
                    }
                }
            }

            if (activeOverlength > BMS_OVER_LENGTH_LIMIT) {
                activeOverlengthCount++;
            }
            if (standbyOverlength > BMS_OVER_LENGTH_LIMIT) {
                standbyOverlengthCount++;
            }
        }
        overview.setActiveOverlengthCount(activeOverlengthCount);
        overview.setStandbyOverlengthCount(standbyOverlengthCount);

        return overview;
    }

    /**
     * 获取断纤场景下的性能概览
     *
     * @param bmsList                   业务复用段集合，
     *                                  存在顺序要求：业务复用段必须是按照源 -> 宿的顺序排列，业务复用段内的OMS必须按照源 -> 宿的顺序排列
     * @param projectSimulationStrategy 项目仿真配置
     */
    private List<BsBusinessRelayOverviewDTO.Item> getFiberBreakingOverview(List<BsBusinessRouteDTO.RouteBms> bmsList,
                                                                           BsProjectSimulationStrategyDTO projectSimulationStrategy) {
        Preconditions.checkNotNull(bmsList);
        Preconditions.checkNotNull(projectSimulationStrategy);

        // 获取所有断纤组合场景
        List<BsBusinessRouteDTO.RouteOms> possibleBrokenOmsList = bmsList.stream()
                .map(BsBusinessRouteDTO.RouteBms::getOmsList)
                .flatMap(List::stream)
                .filter(routeOms -> OmsProtectionMechanismEnum.OMSP.equals(routeOms.getProtectionMechanism()))
                .collect(Collectors.toList());
        List<List<BsBusinessRouteDTO.RouteOms>> fiberBreakingCombinations = CombinationUtils.getAllCombinations(possibleBrokenOmsList, projectSimulationStrategy.getFiberBreakingResistant(), true);

        // 遍历所有断纤组合场景
        List<BsBusinessRelayOverviewDTO.Item> overviewItems = new ArrayList<>();
        for (List<BsBusinessRouteDTO.RouteOms> fiberBreakingCombination : fiberBreakingCombinations) {
            StringBuilder fiberBreakingFlag = new StringBuilder("N");
            List<BigDecimal> bmsOsnrList = new ArrayList<>();
            List<BsBusinessRelayOverviewDTO.BmsOverview> bmsOverviewList = new ArrayList<>();

            for (BsBusinessRouteDTO.RouteBms bms : bmsList) {
                // 设置当前断纤场景下的断纤标识位
                for (BsBusinessRouteDTO.RouteOms routeOms : bms.getOmsList()) {
                    if (fiberBreakingCombination.contains(routeOms)) {
                        fiberBreakingFlag.append(1);
                    } else {
                        fiberBreakingFlag.append(0);
                    }
                }

                List<BsLinkDTO> linkList = this.getLinkListForBms(bms, fiberBreakingCombination);

                // 1、计算当前业务复用段在当前断纤场景下的OSNR值
                // 2、获取当前断纤场景下的OSNR门限值
                List<BigDecimal> omsOsnrList = new ArrayList<>(linkList.size());
                int otsNumber = 0;
                for (BsLinkDTO link : linkList) {
                    // 取较小的OSNR进行计算
                    if (link.getEastOsnrValue().compareTo(link.getWestOsnrValue()) <= 0) {
                        omsOsnrList.add(link.getEastOsnrValue());
                    } else {
                        omsOsnrList.add(link.getWestOsnrValue());
                    }

                    otsNumber += (link.getOaStationNumber() + 1);
                }

                BigDecimal bmsOsnr = osnrFormulaService.calculateLinkOsnr(
                        projectSimulationStrategy.getOsnrFormulaMode(), omsOsnrList);
                BigDecimal osnrThreshold = OsnrThresholdDefinition.getThreshold(otsNumber);

                bmsOsnrList.add(bmsOsnr);

                BsBusinessRelayOverviewDTO.BmsOverview bmsOverview = new BsBusinessRelayOverviewDTO.BmsOverview();
                bmsOverview.setSourceNodeId(bms.getSourceNodeId());
                bmsOverview.setSinkNodeId(bms.getSinkNodeId());
                bmsOverview.setOsnrValue(bmsOsnr);
                bmsOverview.setThresholdValue(osnrThreshold);
                bmsOverviewList.add(bmsOverview);
            }

            BsBusinessRelayOverviewDTO.Item item = new BsBusinessRelayOverviewDTO.Item();
            item.setFiberBreakingFlag(fiberBreakingFlag.toString());
            item.setFiberBreakingOmsIdList(fiberBreakingCombination.stream()
                    .map(BsBusinessRouteDTO.RouteOms::getId)
                    .collect(Collectors.toList()));
            item.setOsnrMin(bmsOsnrList.stream().min(BigDecimal::compareTo).orElse(null));
            item.setOsnrVar(BigDecimalMathUtils.variance(bmsOsnrList.toArray(new BigDecimal[]{}))
                    .setScale(2, RoundingMode.HALF_UP));
            item.setOsnrAve(BigDecimalMathUtils.average(bmsOsnrList.toArray(new BigDecimal[]{}))
                    .setScale(2, RoundingMode.HALF_UP));
            item.setBmsList(bmsOverviewList);

            overviewItems.add(item);
        }

        return overviewItems;
    }

    /**
     * 获取业务复用段的OSNR值
     *
     * @param bms                  业务复用段
     * @param fiberBreakingOmsList 发生断纤的OMS集合
     * @return OSNR值
     */
    private List<BsLinkDTO> getLinkListForBms(BsBusinessRouteDTO.RouteBms bms, List<BsBusinessRouteDTO.RouteOms> fiberBreakingOmsList) {
        Preconditions.checkNotNull(bms);

        if (fiberBreakingOmsList == null) {
            fiberBreakingOmsList = Collections.emptyList();
        }

        List<BsLinkDTO> linkList = new ArrayList<>();
        for (BsBusinessRouteDTO.RouteOms routeOms : bms.getOmsList()) {
            if (fiberBreakingOmsList.contains(routeOms)
                    && OmsProtectionMechanismEnum.OMSP.equals(routeOms.getProtectionMechanism())) {
                if (routeOms.getLinkMap().containsKey(LinkRoleEnum.STANDBY)) {
                    linkList.add(routeOms.getLinkMap().get(LinkRoleEnum.STANDBY));
                }
            } else {
                if (routeOms.getLinkMap().containsKey(LinkRoleEnum.ACTIVE)) {
                    linkList.add(routeOms.getLinkMap().get(LinkRoleEnum.ACTIVE));
                }
            }
        }

        return linkList;
    }

    /**
     * 批量进行业务仿真
     *
     * @param projectId      项目ID
     * @param businessIdList 业务ID集合
     */
    private void doBatchBusinessSimulation(Long projectId,
                                           BsProjectConfigurationDTO projectConfiguration,
                                           List<Long> businessIdList) {
        BsProjectDTO project = bsProjectService.getProjectByProjectId(projectId);

        if (CollectionUtils.isNotEmpty(businessIdList)) {
            List<BsBusinessDO> businessList = bsBusinessMapper.selectBatchIds(businessIdList);

            SimulationCache cache = this.buildCache(projectId);
            for (BsBusinessDO business : businessList) {
                BusinessSimulationArgs args = new BusinessSimulationArgs();
                args.project = project;
                args.projectId = projectId;
                args.business = business;
                args.cache = cache;
                args.projectConfiguration = projectConfiguration;
                args.businessSimulationStrategy = bsBusinessService.getBusinessSimulationStrategy(business.getId());

                this.doSingleBusinessSimulation(args);
            }
        }

        // 设置当前项目阶段、当前项目编辑人
        bsProjectService.updateProjectStage(projectId, null);
        bsProjectMapper.update(null, Wrappers.lambdaUpdate(BsProjectDO.class)
                .set(BsProjectDO::getCurrentEditorId, null)
                .eq(BsProjectDO::getId, projectId));

        // 发送通知
        NotificationDTO notification = new NotificationDTO();
        notification.setType(NotificationTypeEnum.SYSTEM);
        notification.setBizCode(TASK_MANAGER_NOTIFICATION_BIZ_CODE);
        notification.setBizIdentify(projectId.toString());
        notification.setTitle(String.format("项目 \"%s\" 的仿真计算已完成", project.getName()));
        notification.setBizCustom(businessIdList.toString());
        notification.setConsumerType(NotificationConsumerTypeEnum.SPECIFIC_USER);
        if (project.getCurrentEditorId() != null) {
            notificationService.saveNotification(notification, Collections.singletonList(project.getCurrentEditorId()));
        } else {
            notificationService.saveNotification(notification, Collections.singletonList(project.getOwnerUserId()));
        }
    }

    /**
     * 进行单个业务的仿真，包含：1）清理历史仿真信息. 2）进行单个业务仿真. 3）持久化仿真结果.
     *
     * @param args 仿真参数
     */
    @Tx
    protected void doSingleBusinessSimulation(BusinessSimulationArgs args) {
        SimulationLogWrapper logWrapper = new SimulationLogWrapper(args);
        logWrapper.newLog("开始仿真计算");

        logWrapper.newLog("清理上一次仿真计算的结果");
        bsBusinessService.clearBusinessSimulation(Collections.singletonList(args.business.getId()));
        // 清理当前业务历史仿真信息后，更新缓存
        args.cache.update(bsOmsChannelMapper, bsLocalDimensionChannelMapper);

        logWrapper.newLog("计算路由");
        List<BsBusinessRouteDTO> candidateRouteList = this.getCandidateRouteList(args);
        logWrapper.finish(candidateRouteList.size() == 0 ? YesAndNoEnum.NO : YesAndNoEnum.YES,
                "路由条数：" + candidateRouteList.size());

        logWrapper.newLog("开始电再生中继方案计算");
        List<BusinessSimulationResult> results = candidateRouteList.parallelStream()
                .map(route -> this.doSingleBusinessRelayAndChannelSimulation(args, route))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (results.isEmpty()) {
            logWrapper.finish(YesAndNoEnum.NO, "没有可行的电再生中继方案");
        } else {
            logWrapper.finish(YesAndNoEnum.YES, "确定电再生中继方案");
        }

        BusinessSimulationResult result = this.better(args, results);

        logWrapper.newLog("持久化仿真计算结果");
        this.persistentSimulationResult(args, result, candidateRouteList);

        // 保存仿真日志
        logWrapper.finish(YesAndNoEnum.YES, null);
        bsBusinessSimulationLogMapper.insertBatchSomeColumn(logWrapper.logs);
    }

    /**
     * 进行单个业务的中继、波道仿真
     *
     * @param args  仿真参数
     * @param route 仿真路由
     */
    private BusinessSimulationResult doSingleBusinessRelayAndChannelSimulation(BusinessSimulationArgs args, BsBusinessRouteDTO route) {
        Preconditions.checkNotNull(args);
        Preconditions.checkNotNull(route);

        List<BsCrossDomainRelationshipDTO> differentNodeCrossDomainRelationshipList = route.getDifferentNodeCrossDomainRelationshipList();
        Map<Long, List<BsCrossDomainRelationshipDTO>> nodeIdCrossDomainRelationshipMap = new HashMap<>(8);
        for (BsCrossDomainRelationshipDTO relationship : differentNodeCrossDomainRelationshipList) {
            nodeIdCrossDomainRelationshipMap.computeIfAbsent(relationship.getSourceNodeId(), k -> new ArrayList<>()).add(relationship);
            nodeIdCrossDomainRelationshipMap.computeIfAbsent(relationship.getSinkNodeId(), k -> new ArrayList<>()).add(relationship);
        }

        List<Long> definiteRelayNodeIdList = new ArrayList<>();
        List<Long> possibleRelayNodeIdList = new ArrayList<>();
        for (BsBusinessRouteDTO.RouteNode routeNode : route.getNodeList()) {
            if (route.getBeginNode().getId().equals(routeNode.getId())) {
                continue;
            }
            if (route.getEndNode().getId().equals(routeNode.getId())) {
                continue;
            }

            if (NodeTypeEnum.ROADM.equals(routeNode.getType())) {
                if (nodeIdCrossDomainRelationshipMap.containsKey(routeNode.getId())) {
                    definiteRelayNodeIdList.add(routeNode.getId());
                    continue;
                }
                possibleRelayNodeIdList.add(routeNode.getId());
            } else if (NodeTypeEnum.OTM.equals(routeNode.getType())) {
                definiteRelayNodeIdList.add(routeNode.getId());
            }
        }

        List<List<Long>> allCombinations = CombinationUtils.getAllCombinations(
                possibleRelayNodeIdList, true);

        // 按照中继数量分组
        Map<Integer, List<List<Long>>> combinationGroupedSizeMap = allCombinations.stream()
                .collect(Collectors.groupingBy(List::size));

        // 从没有中继开始依次尝试中继数量
        for (int i = 0; i <= possibleRelayNodeIdList.size(); i++) {
            List<List<Long>> combinations = combinationGroupedSizeMap.getOrDefault(i, Collections.emptyList());

            List<BusinessSimulationResult> results = combinations.parallelStream().map(combination -> {
                List<Long> relayNodeIdList = CollectionUtils.collate(combination, definiteRelayNodeIdList);

                boolean meetsForcedRelayDistanceRequirement = true;
                Long forcedRelayDistance = args.businessSimulationStrategy.getForcedRelayDistance();
                if (forcedRelayDistance == null) {
                    forcedRelayDistance = args.projectConfiguration.getSimulationStrategy().getForcedRelayDistance();
                }
                if (forcedRelayDistance == null) {
                    forcedRelayDistance = Long.MAX_VALUE;
                }
                List<BsBusinessRouteDTO.RouteBms> bmsList = BsBusinessRouteUtils.getBmsList(route, relayNodeIdList);
                for (BsBusinessRouteDTO.RouteBms bms : bmsList) {
                    Long distance = 0L;
                    for (BsBusinessRouteDTO.RouteOms oms : bms.getOmsList()) {
                        BsLinkDTO activeLink = oms.getLinkMap().get(LinkRoleEnum.ACTIVE);
                        distance += activeLink.getDistance();
                    }

                    if (distance > forcedRelayDistance) {
                        meetsForcedRelayDistanceRequirement = false;
                        break;
                    }
                }
                if (!meetsForcedRelayDistanceRequirement) {
                    // 中继方案不满足强制中继距离
                    return null;
                }

                BsBusinessRelayOverviewDTO relayOverview = this.getBusinessRelayOverview(
                        args.projectConfiguration.getSimulationStrategy(), route, relayNodeIdList);
                if (YesAndNoEnum.NO.equals(relayOverview.getSatisfied())) {
                    return null;
                }

                ChannelSimulationResult channelResult = this.getChannelSimulationResult(args, route, relayNodeIdList);
                if (channelResult == null) {
                    // 中继方案波道不满足要求
                    return null;
                }

                BusinessSimulationResult result = new BusinessSimulationResult();
                result.route = route;
                result.relay = relayOverview;
                result.channel = channelResult;
                return result;
            }).filter(Objects::nonNull).collect(Collectors.toList());

            // 每组比较完再确认是否计算成功，目的是拿到相同中继数量下，最优的方案
            BusinessSimulationResult result = results.isEmpty() ? null : results.get(0);
            for (BusinessSimulationResult item : results) {
                if (!result.relay.equals(this.better(result.relay, item.relay))) {
                    result = item;
                }
            }

            if (result != null) {
                return result;
            }
        }

        return null;
    }

    /**
     * 获取波道仿真结果
     *
     * @param args            仿真参数
     * @param route           业务路由
     * @param relayNodeIdList 中继站方案
     * @return 波道仿真结果
     */
    private ChannelSimulationResult getChannelSimulationResult(BusinessSimulationArgs args, BsBusinessRouteDTO route, List<Long> relayNodeIdList) {
        SimulationCache cache = args.cache;

        List<BsBusinessRouteDTO.RouteBms> bmsList = BsBusinessRouteUtils.getBmsList(route, relayNodeIdList);
        List<List<Integer>> bmsAvailableOmsChannelsList = new ArrayList<>(bmsList.size());
        for (BsBusinessRouteDTO.RouteBms bms : bmsList) {
            bmsAvailableOmsChannelsList.add(this.getAvailableOmsChannelForBms(cache, bms));
        }

        // 尝试不切换波长
        Collection<Integer> allBmsAvailableOmsChannels = new ArrayList<>(args.cache.allChannelList);
        for (List<Integer> bmsAvailableOmsChannels : bmsAvailableOmsChannelsList) {
            allBmsAvailableOmsChannels = CollectionUtils.intersection(allBmsAvailableOmsChannels, bmsAvailableOmsChannels);
        }

        if (allBmsAvailableOmsChannels.isEmpty()) {
            return null;
        }

        for (Integer channelNumber : allBmsAvailableOmsChannels.stream().sorted().collect(Collectors.toList())) {
            Map<BsBusinessRouteDTO.RouteBms, Integer> bmsChannelMap = new HashMap<>(bmsList.size());
            for (BsBusinessRouteDTO.RouteBms bms : bmsList) {
                bmsChannelMap.put(bms, channelNumber);
            }

            ChannelSimulationResult result = getChannelSimulationResult(args, bmsList, bmsChannelMap);
            if (result != null) {
                return result;
            }
        }

        // 不切换波长不行时，尝试切换波长
        if (YesAndNoEnum.YES.equals(args.projectConfiguration.getSimulationStrategy().getAllowSwitchingChannel())) {
            AtomicReference<ChannelSimulationResult> resultRef = new AtomicReference<>();
            Stream<List<Integer>> parallelStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    new BmsChannelSelectionIterator(args.projectConfiguration.getSimulationStrategy().getAllowSwitchingChannel(),
                            bmsAvailableOmsChannelsList), 0), true);

            parallelStream.forEach(bmsChannelCombination -> {
                Map<BsBusinessRouteDTO.RouteBms, Integer> bmsChannelMap = new HashMap<>(bmsList.size());
                for (int i = 0, bmsChannelCombinationSize = bmsChannelCombination.size(); i < bmsChannelCombinationSize; i++) {
                    Integer channel = bmsChannelCombination.get(i);
                    bmsChannelMap.put(bmsList.get(i), channel);
                }

                ChannelSimulationResult option = getChannelSimulationResult(args, bmsList, bmsChannelMap);
                if (option != null) {
                    if (resultRef.get() == null || resultRef.get().sameOmsChannelCount < option.sameOmsChannelCount) {
                        resultRef.set(option);
                    }
                }
            });

            return resultRef.get();
        }

        return null;
    }

    /**
     * 获取完整的波长配置方案
     *
     * @return
     */
    private ChannelSimulationResult getChannelSimulationResult(BusinessSimulationArgs args,
                                                               List<BsBusinessRouteDTO.RouteBms> bmsList,
                                                               Map<BsBusinessRouteDTO.RouteBms, Integer> bmsChannelMap) {
        ChannelSimulationResult result = new ChannelSimulationResult();

        Map<Long, List<BsBusinessRouteDTO.RouteBms>> bmsListGroupedByNodeId = new HashMap<>(bmsChannelMap.size() * 2);
        for (BsBusinessRouteDTO.RouteBms bms : bmsList) {
            bmsListGroupedByNodeId.computeIfAbsent(bms.getSourceNodeId(), k -> new ArrayList<>()).add(bms);
            bmsListGroupedByNodeId.computeIfAbsent(bms.getSinkNodeId(), k -> new ArrayList<>()).add(bms);
        }

        Map<Long, BsDistributionTypeEnum> nodeDistributionTypeMap = new HashMap<>(bmsListGroupedByNodeId.size());
        for (Map.Entry<Long, List<BsBusinessRouteDTO.RouteBms>> entry : bmsListGroupedByNodeId.entrySet()) {
            BsNodeDO node = args.cache.nodeMap.get(entry.getKey());
            List<BsBusinessRouteDTO.RouteBms> bmsListByNode = entry.getValue();

            if (bmsListByNode.size() == 1) {
                nodeDistributionTypeMap.put(node.getId(), BsDistributionTypeEnum.DESCENT);

                BsLocalDimensionDO localDimension = null;
                Integer channel = bmsChannelMap.get(bmsListByNode.get(0));
                if (NodeTypeEnum.ROADM.getCode() == node.getType()) {
                    List<BsLocalDimensionDO> localDimensionList = availableLocalDimensionForRoadmDescent(args.cache, node, channel);
                    if (CollectionUtils.isEmpty(localDimensionList)) {
                        return null;
                    } else {
                        localDimension = localDimensionList.get(0);
                    }
                } else if (NodeTypeEnum.OTM.getCode() == node.getType()) {
                    localDimension = availableLocalDimensionForOtmDescent(args.cache, node, channel, bmsListByNode.get(0).getOmsList().get(0).getId());
                } else {
                    // 不应该出现其他的网元类型
                    throw new IllegalArgumentException();
                }

                if (localDimension == null) {
                    return null;
                }

                BsLocalDimensionChannelDO localDimensionChannel = new BsLocalDimensionChannelDO();
                localDimensionChannel.setBsProjectId(args.project.getId());
                localDimensionChannel.setBsNodeId(node.getId());
                localDimensionChannel.setBsLocalDimensionId(localDimension.getId());
                localDimensionChannel.setBsBusinessId(args.business.getId());
                localDimensionChannel.setChannelIndex(channel);
                localDimensionChannel.setOccupyType(args.business.getType());
                localDimensionChannel.setDistributionType(nodeDistributionTypeMap.get(node.getId()).getCode());
                if (!Objects.equals(node.getId(), args.business.getSourceNodeId()) && !Objects.equals(node.getId(), args.business.getSinkNodeId())) {
                    localDimensionChannel.setCrossDomain(YesAndNoEnum.YES.getCode());
                } else {
                    localDimensionChannel.setCrossDomain(YesAndNoEnum.NO.getCode());
                }

                result.localDimensionChannelOccupied.add(localDimensionChannel);
            } else if (bmsListByNode.size() == 2) {
                nodeDistributionTypeMap.put(node.getId(), BsDistributionTypeEnum.RELAY);
                /*
                 * (1) ------- (2) ------- (3)
                 *     -：oms     (): 节点
                 * 相较于节点(2)，左边的多个OMS组成 bmsLeft，右边的多个OMS组成 bmsRight
                 */
                BsBusinessRouteDTO.RouteBms bmsLeft, bmsRight;
                if (bmsListByNode.get(0).getSourceNodeId().equals(node.getId())) {
                    bmsLeft = bmsListByNode.get(1);
                    bmsRight = bmsListByNode.get(0);
                } else {
                    bmsLeft = bmsListByNode.get(0);
                    bmsRight = bmsListByNode.get(1);
                }

                Integer channelLeft = bmsChannelMap.get(bmsLeft);
                Integer channelRight = bmsChannelMap.get(bmsRight);

                BsLocalDimensionDO localDimensionLeft = null;
                BsLocalDimensionDO localDimensionRight = null;
                if (NodeTypeEnum.ROADM.getCode() == node.getType()) {
                    List<List<BsLocalDimensionDO>> localDimensionGroupList = availableLocalDimensionForRoadmRelay(args.cache, node, channelLeft, channelRight);
                    if (CollectionUtils.isNotEmpty(localDimensionGroupList)) {
                        List<BsLocalDimensionDO> localDimensionGroup = localDimensionGroupList.get(0);
                        localDimensionLeft = localDimensionGroup.get(0);
                        localDimensionRight = localDimensionGroup.get(1);
                    }
                } else if (NodeTypeEnum.OTM.getCode() == node.getType()) {
                    List<BsLocalDimensionDO> localDimensionList = availableLocalDimensionForOtmRelay(args.cache, node,
                            bmsLeft.getOmsList().get(bmsLeft.getOmsList().size() - 1).getId(), channelLeft,
                            bmsRight.getOmsList().get(0).getId(), channelRight);
                    if (CollectionUtils.isNotEmpty(localDimensionList)) {
                        localDimensionLeft = localDimensionList.get(0);
                        localDimensionRight = localDimensionList.get(1);
                    }
                } else {
                    // 不应该出现其他的网元类型
                    throw new IllegalArgumentException();
                }

                if (localDimensionLeft == null || localDimensionRight == null) {
                    return null;
                }

                BsLocalDimensionChannelDO localDimensionLeftChannel = new BsLocalDimensionChannelDO();
                localDimensionLeftChannel.setBsProjectId(args.project.getId());
                localDimensionLeftChannel.setBsNodeId(node.getId());
                localDimensionLeftChannel.setBsLocalDimensionId(localDimensionLeft.getId());
                localDimensionLeftChannel.setBsBusinessId(args.business.getId());
                localDimensionLeftChannel.setChannelIndex(channelLeft);
                localDimensionLeftChannel.setOccupyType(args.business.getType());
                localDimensionLeftChannel.setDistributionType(BsDistributionTypeEnum.RELAY.getCode());
                localDimensionLeftChannel.setCrossDomain(YesAndNoEnum.NO.getCode());
                localDimensionLeftChannel.setTargetLocalDimensionId(localDimensionRight.getId());

                BsLocalDimensionChannelDO localDimensionRightChannel = new BsLocalDimensionChannelDO();
                localDimensionRightChannel.setBsProjectId(args.project.getId());
                localDimensionRightChannel.setBsNodeId(node.getId());
                localDimensionRightChannel.setBsLocalDimensionId(localDimensionRight.getId());
                localDimensionRightChannel.setBsBusinessId(args.business.getId());
                localDimensionRightChannel.setChannelIndex(channelRight);
                localDimensionRightChannel.setOccupyType(args.business.getType());
                localDimensionRightChannel.setDistributionType(BsDistributionTypeEnum.RELAY.getCode());
                localDimensionRightChannel.setCrossDomain(YesAndNoEnum.NO.getCode());
                localDimensionRightChannel.setTargetLocalDimensionId(localDimensionLeft.getId());

                result.localDimensionChannelOccupied.add(localDimensionLeftChannel);
                result.localDimensionChannelOccupied.add(localDimensionRightChannel);
            } else {
                // 不应该出现其他数量
                throw new IllegalArgumentException();
            }
        }

        HashMultiset<Integer> channelCountSet = HashMultiset.create();
        for (BsBusinessRouteDTO.RouteBms bms : bmsList) {
            Integer bmsChannel = bmsChannelMap.get(bms);
            for (BsBusinessRouteDTO.RouteOms routeOms : bms.getOmsList()) {
                BsOmsDO oms = args.cache.omsMap.get(routeOms.getId());
                List<Integer> availableOmsChannel = args.cache.availableOmsChannel.getOrDefault(oms, Collections.emptyList());
                if (availableOmsChannel.contains(bmsChannel)) {
                    channelCountSet.add(bmsChannel);

                    BsOmsChannelDO omsChannel = new BsOmsChannelDO();
                    omsChannel.setBsProjectId(args.project.getId());
                    omsChannel.setBsOmsId(oms.getId());
                    omsChannel.setBsBusinessId(args.business.getId());
                    omsChannel.setChannelIndex(bmsChannel);
                    omsChannel.setOccupyType(args.business.getType());
                    omsChannel.setConsumesBandwidth(args.business.getBandwidth());
                    omsChannel.setSourceNodeDistributionType(
                            nodeDistributionTypeMap.getOrDefault(oms.getSourceNodeId(), BsDistributionTypeEnum.THROUGH).getCode());
                    omsChannel.setSinkNodeDistributionType(
                            nodeDistributionTypeMap.getOrDefault(oms.getSinkNodeId(), BsDistributionTypeEnum.THROUGH).getCode());

                    result.omsChannelOccupied.add(omsChannel);
                } else {
                    return null;
                }
            }
        }

        for (Multiset.Entry<Integer> entry : channelCountSet.entrySet()) {
            if (result.sameOmsChannelCount < entry.getCount()) {
                result.sameOmsChannelCount = entry.getCount();
            }
        }

        return result;
    }

    /**
     * 获取业务复用段的可用波道号
     *
     * @param cache    缓存
     * @param routeBms 业务复用段
     * @return 可用波道号集合
     */
    private List<Integer> getAvailableOmsChannelForBms(SimulationCache cache, BsBusinessRouteDTO.RouteBms routeBms) {
        Map<Long, BsOmsDO> omsMap = cache.omsMap;

        Collection<Integer> availableOmsChannelList = new HashSet<>(cache.allChannelList);
        for (BsBusinessRouteDTO.RouteOms routeOms : routeBms.getOmsList()) {
            BsOmsDO oms = omsMap.get(routeOms.getId());
            List<Integer> currentOmsAvailableChannelList = cache.availableOmsChannel.getOrDefault(oms,
                    Collections.emptyList());

            availableOmsChannelList = CollectionUtils.intersection(currentOmsAvailableChannelList, availableOmsChannelList);
        }

        return new ArrayList<>(availableOmsChannelList);
    }

    /**
     * 获取ROADM节点中可用于指定波长落地的本地维度
     *
     * @param cache   缓存
     * @param node    节点
     * @param channel 波道号
     * @return 本地维度集合
     */
    private List<BsLocalDimensionDO> availableLocalDimensionForRoadmDescent(SimulationCache cache,
                                                                            BsNodeDO node,
                                                                            Integer channel) {
        List<BsLocalDimensionDO> result = new ArrayList<>();
        List<BsLocalDimensionDO> localDimensionList = cache.nodeLocalDimensionMap.getOrDefault(node,
                Collections.emptyList());

        if (localDimensionList.isEmpty()) {
            return Collections.emptyList();
        }

        for (BsLocalDimensionDO localDimension : localDimensionList) {
            List<Integer> availableChannel = cache.availableLocalDimensionChannel.getOrDefault(localDimension,
                    Collections.emptyList());
            if (availableChannel.contains(channel)) {
                result.add(localDimension);
            }
        }

        // 没有分组的在前面，创建时间早的在前面
        result.sort(Collections.reverseOrder((o1, o2) -> {
            boolean b1 = o1.getIndex() == null;
            boolean b2 = o2.getIndex() == null;
            if (b1 == b2) {
                return o2.getCreatedTime().compareTo(o1.getCreatedTime());
            } else if (b1) {
                return 1;
            } else {
                return -1;
            }
        }));

        return result;
    }

    /**
     * 获取ROADM节点中可用于指定波长中继的本地维度分组集合
     *
     * @param cache         缓存
     * @param node          节点
     * @param sourceChannel 波道来源的波道号
     * @param targetChannel 波道去处的波道号
     * @return 本地维度分组集合，<b>注意：每个分组按照源波道本地维度、目标波道本地维度的顺序排列</b>
     */
    private List<List<BsLocalDimensionDO>> availableLocalDimensionForRoadmRelay(SimulationCache cache,
                                                                                BsNodeDO node,
                                                                                Integer sourceChannel,
                                                                                Integer targetChannel) {
        List<BsLocalDimensionDO> localDimensionList = cache.nodeLocalDimensionMap.getOrDefault(node,
                Collections.emptyList());

        localDimensionList.removeIf(ld -> StringUtils.isBlank(ld.getIndex()));
        if (localDimensionList.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, List<BsLocalDimensionDO>> ldListGroupByIndex = localDimensionList.stream()
                .collect(Collectors.groupingBy(BsLocalDimensionDO::getIndex, Collectors.toList()));

        List<List<BsLocalDimensionDO>> result = new ArrayList<>();
        for (Map.Entry<String, List<BsLocalDimensionDO>> entry : ldListGroupByIndex.entrySet()) {
            BsLocalDimensionDO sourceLd = null, targetLd = null;
            for (BsLocalDimensionDO ld : entry.getValue()) {
                List<Integer> availableChannelByLd = cache.availableLocalDimensionChannel.getOrDefault(ld,
                        Collections.emptyList());

                if (sourceLd == null && availableChannelByLd.contains(sourceChannel)) {
                    sourceLd = ld;
                } else if (targetLd == null && availableChannelByLd.contains(targetChannel)) {
                    targetLd = ld;
                }
            }

            if (sourceLd != null && targetLd != null) {
                // 此处add的顺序需注意，外部取用时应按照该约定
                result.add(Arrays.asList(sourceLd, targetLd));
            }
        }

        return result;
    }

    /**
     * 获取OTM节点中可用于指定波长落地的本地维度
     *
     * @param cache       缓存
     * @param node        节点
     * @param channel     波道号
     * @param sourceOmsId 波道来源的OMS唯一标识
     * @return 本地维度
     */
    private BsLocalDimensionDO availableLocalDimensionForOtmDescent(SimulationCache cache,
                                                                    BsNodeDO node,
                                                                    Integer channel,
                                                                    Long sourceOmsId) {
        List<BsLocalDimensionDO> localDimensionList = cache.nodeLocalDimensionMap.getOrDefault(node,
                Collections.emptyList());

        if (localDimensionList.isEmpty()) {
            return null;
        }

        for (BsLocalDimensionDO localDimension : localDimensionList) {
            if (!Objects.equals(localDimension.getBsOmsId(), sourceOmsId)) {
                continue;
            }

            List<Integer> availableChannel = cache.availableLocalDimensionChannel.getOrDefault(localDimension,
                    Collections.emptyList());
            if (availableChannel.contains(channel)) {
                return localDimension;
            } else {
                return null;
            }
        }

        // 没有该线路方向的本地维度
        return null;
    }

    /**
     * 获取OTM节点中可用于指定波长中继至指定节点的本地维度组合集合
     *
     * @param cache         缓存
     * @param node          节点
     * @param sourceOmsId   波道来源的OMS唯一标识
     * @param sourceChannel 波道来源的波道号
     * @param targetOmsId   波道去处的OMS唯一标识
     * @param targetChannel 波道去处的波道号
     * @return 本地维度集合
     */
    private List<BsLocalDimensionDO> availableLocalDimensionForOtmRelay(SimulationCache cache,
                                                                        BsNodeDO node,
                                                                        Long sourceOmsId,
                                                                        Integer sourceChannel,
                                                                        Long targetOmsId,
                                                                        Integer targetChannel) {
        List<BsLocalDimensionDO> localDimensionList = cache.nodeLocalDimensionMap.getOrDefault(node,
                Collections.emptyList());

        if (localDimensionList.isEmpty()) {
            return Collections.emptyList();
        }

        BsLocalDimensionDO sourceLd = null, targetLd = null;
        for (BsLocalDimensionDO localDimension : localDimensionList) {
            List<Integer> availableChannel = cache.availableLocalDimensionChannel.getOrDefault(localDimension,
                    Collections.emptyList());

            if (Objects.equals(localDimension.getBsOmsId(), sourceOmsId)) {
                if (availableChannel.contains(sourceChannel)) {
                    sourceLd = localDimension;
                }
            } else if (Objects.equals(localDimension.getBsOmsId(), targetOmsId)) {
                if (availableChannel.contains(targetChannel)) {
                    targetLd = localDimension;
                }
            }
        }

        if (sourceLd != null && targetLd != null) {
            return Arrays.asList(sourceLd, targetLd);
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * 从仿真结果中，选择最优得那一个返回
     *
     * @param args    仿真参数
     * @param results 仿真结果集合
     * @return 最优的仿真结果
     */
    private BusinessSimulationResult better(BusinessSimulationArgs args, List<BusinessSimulationResult> results) {
        if (CollectionUtils.isEmpty(results)) {
            return null;
        }
        if (results.size() == 1) {
            return results.get(0);
        }
        StandardCache cache = this.standardData(args, results);
        RoutePolicyEnum routePolicy;
        if (args.businessSimulationStrategy.getRoutePolicy() != null) {
            routePolicy = args.businessSimulationStrategy.getRoutePolicy();
        } else if (args.projectConfiguration.getSimulationStrategy().getRoutePolicy() != null) {
            routePolicy = args.projectConfiguration.getSimulationStrategy().getRoutePolicy();
        } else {
            routePolicy = RoutePolicyEnum.SHORTEST_PATH;
        }

        BsRoutePolicyDO routePolicyParameter = policyMapper.selectOne(Wrappers.lambdaQuery(BsRoutePolicyDO.class)
                .eq(BsRoutePolicyDO::getRoutePolicy, routePolicy.getCode()));

        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> allScore =
                this.score(cache, routePolicyParameter, results);
        DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal> minData = allScore.stream()
                .min(Comparator.comparing(DataStandardDeviationUtil.Data::getValue))
                .orElse(null);

        return minData == null ? null : minData.getKey();
    }


    /**
     * 计算分数
     * // TODO 跨域次数
     *
     * @param cache   标准化数据
     * @param policy  路由策略
     * @param results 仿真结果
     * @return 仿真结果分数
     */
    private List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> score(StandardCache cache,
                                                                                             BsRoutePolicyDO policy,
                                                                                             List<BusinessSimulationResult> results) {
        Preconditions.checkNotNull(cache);
        Preconditions.checkNotNull(policy);

        if (CollectionUtils.isEmpty(results)) {
            return Collections.emptyList();
        }
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> allScore
                = new ArrayList<>();
        for (BusinessSimulationResult result : results) {
            BigDecimal distance = cache.distanceStandardDataMap.get(result);
            BigDecimal distanceScore = distance.multiply(policy.getDistanceWeight());

            BigDecimal ots = cache.otsStandardDataMap.get(result);
            BigDecimal otsScore = ots.multiply(policy.getOtsNumberWeight());

            BigDecimal oms = cache.omsStandardDataMap.get(result);
            BigDecimal omsScore = oms.multiply(policy.getOmsNumberWeight());

            BigDecimal performance = cache.performanceDataMap.get(result);
            BigDecimal performanceScore = performance.multiply(policy.getOsnrWeight());

            BigDecimal omsUtilization = cache.omsUtilizationDataMap.get(result);
            BigDecimal omsUtilizationScore = omsUtilization.multiply(policy.getOmsUtilizationWeight());

            BigDecimal relay = cache.relayStandardDataMap.get(result);
            BigDecimal relayScore = relay.multiply(policy.getRelayNumberWeight());

            BigDecimal total = distanceScore
                    .add(otsScore)
                    .add(omsScore)
                    .add(performanceScore)
                    .add(omsUtilizationScore)
                    .add(relayScore);
            allScore.add(new DataStandardDeviationUtil.Data<>(result, total));
        }
        return allScore;
    }


    /**
     * 标砖化距离数据
     *
     * @param args    仿真参数
     * @param results 仿真结果
     * @return 标准化的数据
     */
    private StandardCache standardData(
            BusinessSimulationArgs args,
            List<BusinessSimulationResult> results) {
        if (CollectionUtils.isEmpty(results)) {
            return null;
        }
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> omsData = new ArrayList<>();
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> otsData = new ArrayList<>();
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> distanceData = new ArrayList<>();
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> relayData = new ArrayList<>();

        for (BusinessSimulationResult result : results) {
            List<BsBusinessRouteDTO.RouteOms> omsList = result.route.getOmsList();

            long allDistance = 0L;
            long allOtsNCount = 0L;
            for (BsBusinessRouteDTO.RouteOms routeOms : omsList) {
                BsLinkDTO activeLink = args.cache.omsIdActiveLinkMap.get(routeOms.getId());
                Long distance = activeLink.getDistance();
                allDistance = allDistance + distance;

                int otsNCount = activeLink.getOaStationNumber() + 1;
                allOtsNCount = allOtsNCount + otsNCount;
            }
            distanceData.add(new DataStandardDeviationUtil.Data<>(result, new BigDecimal(allDistance)));
            otsData.add(new DataStandardDeviationUtil.Data<>(result, new BigDecimal(allOtsNCount)));
            omsData.add(new DataStandardDeviationUtil.Data<>(result, new BigDecimal(omsList.size())));
            relayData.add(new DataStandardDeviationUtil.Data<>(result,
                    new BigDecimal(result.relay.getRelayNodeIdList().size())));
        }
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> omsUtilizationData =
                this.getOmsUtilizationData(args, results);
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> performanceData =
                this.getPerformanceData(results);
        return new StandardCache(
                DataStandardDeviationUtil.zScore(distanceData),
                DataStandardDeviationUtil.zScore(otsData),
                DataStandardDeviationUtil.zScore(omsData),
                DataStandardDeviationUtil.zScore(relayData),
                DataStandardDeviationUtil.zScore(omsUtilizationData),
                DataStandardDeviationUtil.zScore(performanceData)
        );
    }

    /**
     * 获取OMS利用率
     *
     * @param args    仿真参数
     * @param results 仿真结果
     * @return OMS利用率
     */
    private List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>>
    getOmsUtilizationData(BusinessSimulationArgs args, List<BusinessSimulationResult> results) {
        if (CollectionUtils.isEmpty(results)) {
            return null;
        }
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> relayData = new ArrayList<>();
        for (BusinessSimulationResult result : results) {
            List<BsBusinessRouteDTO.RouteOms> omsList = result.route.getOmsList();
            int allUsedChannelNum = 0;
            int allChannelNum = 0;
            for (BsBusinessRouteDTO.RouteOms routeOms : omsList) {
                BsOmsDO bsOmsDO = args.cache.omsMap.get(routeOms.getId());
                List<Integer> integerList = args.cache.availableOmsChannel.get(bsOmsDO);
                List<Integer> allChannelList = args.cache.allChannelList;
                int usedChannelNum = allChannelList.size() - integerList.size();

                allChannelNum = allChannelNum + allChannelList.size();
                allUsedChannelNum = allUsedChannelNum + usedChannelNum;
            }
            BigDecimal omsUtilization = new BigDecimal(allUsedChannelNum).divide(new BigDecimal(allChannelNum),
                    RoundingMode.HALF_UP);
            relayData.add(new DataStandardDeviationUtil.Data<>(result, omsUtilization));
        }
        return relayData;
    }

    /**
     * 获取OSNR性能数据
     *
     * @param results 仿真参数
     * @return OSNR性能数据
     */
    private List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>>
    getPerformanceData(List<BusinessSimulationResult> results) {
        if (CollectionUtils.isEmpty(results)) {
            return null;
        }
        List<BusinessSimulationResult> newResults = new ArrayList<>(results);
        newResults.sort((r1, r2) -> {
            if (r1.equals(r2)) {
                return 0;
            }
            BsBusinessRelayOverviewDTO res = this.betterOsnr(r1.relay, r2.relay);
            if (res.equals(r1.relay)) {
                return -1;
            } else {
                return 1;
            }
        });
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> performanceData
                = new ArrayList<>();
        BigDecimal time = new BigDecimal("0");
        for (BusinessSimulationResult newResult : newResults) {
            performanceData.add(new DataStandardDeviationUtil.Data<>(newResult,
                    time.add(new BigDecimal("1"))));
        }
        return performanceData;
    }

    /**
     * 比较两个中继方案的概况，并返回性能更优的那一个
     *
     * @param a 中继方案概况a
     * @param b 中继方案概况b
     * @return 性能更优的中继方案概况
     */
    private BsBusinessRelayOverviewDTO better(BsBusinessRelayOverviewDTO a, BsBusinessRelayOverviewDTO b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }

        // 比较中继数量
        if (a.getRelayNodeIdList().size() > b.getRelayNodeIdList().size()) {
            return b;
        } else if (a.getRelayNodeIdList().size() < b.getRelayNodeIdList().size()) {
            return a;
        }
        return this.betterOsnr(a, b);
    }

    /**
     * 比较两个中继方案osnr性能
     *
     * @param a 中继方案概况a
     * @param b 中继方案概况a
     * @return osnr性能更优的中继方案概况
     */
    private BsBusinessRelayOverviewDTO betterOsnr(BsBusinessRelayOverviewDTO a, BsBusinessRelayOverviewDTO b) {
        List<List<BigDecimal>> aOsnrDifferenceValue = a.getItems().stream().map(item -> item.getBmsList().stream()
                .map(bmsOverview -> bmsOverview.getOsnrValue().subtract(bmsOverview.getThresholdValue()))
                .collect(Collectors.toList())).collect(Collectors.toList());

        List<List<BigDecimal>> bOsnrDifferenceValue = b.getItems().stream().map(item -> item.getBmsList().stream()
                .map(bmsOverview -> bmsOverview.getOsnrValue().subtract(bmsOverview.getThresholdValue()))
                .collect(Collectors.toList())).collect(Collectors.toList());

        // 比较OSNR门限差值的最小值
        BigDecimal aMinOsnrDifferenceValue = aOsnrDifferenceValue.stream()
                .map(osnrDifferenceValue -> osnrDifferenceValue.stream().min(BigDecimal::compareTo).orElse(BigDecimal.ZERO))
                .min(BigDecimal::compareTo).orElse(BigDecimal.ZERO);

        BigDecimal bMinOsnrDifferenceValue = bOsnrDifferenceValue.stream()
                .map(osnrDifferenceValue -> osnrDifferenceValue.stream().min(BigDecimal::compareTo).orElse(BigDecimal.ZERO))
                .min(BigDecimal::compareTo).orElse(BigDecimal.ZERO);

        if (aMinOsnrDifferenceValue.compareTo(bMinOsnrDifferenceValue) < 0) {
            return b;
        } else if (aMinOsnrDifferenceValue.compareTo(bMinOsnrDifferenceValue) > 0) {
            return a;
        }
        // 比较OSNR门限差值的均方差
        List<BigDecimal> aBmsDifferenceValueAverageVariance = aOsnrDifferenceValue.stream()
                .map(osnrDifferenceValue -> BigDecimalMathUtils.variance(osnrDifferenceValue.toArray(new BigDecimal[]{})))
                .collect(Collectors.toList());

        BigDecimal aOsnrDifferenceValueAverageVariance = BigDecimalMathUtils
                .variance(aBmsDifferenceValueAverageVariance.toArray(new BigDecimal[]{}));


        List<BigDecimal> bBmsDifferenceValueAverageVariance = bOsnrDifferenceValue.stream()
                .map(osnrDifferenceValue -> BigDecimalMathUtils.variance(osnrDifferenceValue.toArray(new BigDecimal[]{})))
                .collect(Collectors.toList());

        BigDecimal bOsnrDifferenceValueAverageVariance = BigDecimalMathUtils
                .variance(bBmsDifferenceValueAverageVariance.toArray(new BigDecimal[]{}));

        if (aOsnrDifferenceValueAverageVariance.compareTo(bOsnrDifferenceValueAverageVariance) < 0) {
            return a;
        } else if (aOsnrDifferenceValueAverageVariance.compareTo(bOsnrDifferenceValueAverageVariance) > 0) {
            return b;
        }
        // 比较OSNR门限差值的平均值
        List<BigDecimal> aBmsDifferenceValueAverage = aOsnrDifferenceValue.stream()
                .map(osnrDifferenceValue -> BigDecimalMathUtils.average(osnrDifferenceValue.toArray(new BigDecimal[]{})))
                .collect(Collectors.toList());

        BigDecimal aOsnrDifferenceValueAverage = BigDecimalMathUtils
                .variance(aBmsDifferenceValueAverage.toArray(new BigDecimal[]{}));


        List<BigDecimal> bBmsDifferenceValueAverage = bOsnrDifferenceValue.stream()
                .map(osnrDifferenceValue -> BigDecimalMathUtils.average(osnrDifferenceValue.toArray(new BigDecimal[]{})))
                .collect(Collectors.toList());

        BigDecimal bOsnrDifferenceValueAverage = BigDecimalMathUtils
                .variance(bBmsDifferenceValueAverage.toArray(new BigDecimal[]{}));

        if (aOsnrDifferenceValueAverage.compareTo(bOsnrDifferenceValueAverage) < 0) {
            return b;
        } else if (aOsnrDifferenceValueAverage.compareTo(bOsnrDifferenceValueAverage) > 0) {
            return a;
        }
        return a;
    }

    /**
     * 获取候选的路由集合
     *
     * @param args 仿真参数
     * @return 路由集合
     */
    private List<BsBusinessRouteDTO> getCandidateRouteList(BusinessSimulationArgs args) {
        BsBusinessDO business = args.business;
        Map<Long, BsNodeDO> nodeMap = args.cache.nodeMap;

        Graph<BsNodeDO, GraphEdge> graph = this.buildGraph(args);
        if (!graph.containsVertex(nodeMap.get(business.getSourceNodeId())) ||
                !graph.containsVertex(nodeMap.get(business.getSinkNodeId()))) {
            return Collections.emptyList();
        }

        List<GraphPath<BsNodeDO, GraphEdge>> kShortestPath;
        List<Long> omsIdList = getOmsIdListOnPathComposedOfDesignatedPoints(args);
        if (CollectionUtils.isNotEmpty(omsIdList)) {
            Set<GraphEdge> beRemovedEdgeSet = new HashSet<>();
            for (GraphEdge graphEdge : graph.edgeSet()) {
                if (graphEdge.oms != null && !omsIdList.contains(graphEdge.oms.getId())) {
                    beRemovedEdgeSet.add(graphEdge);
                }
            }
            beRemovedEdgeSet.forEach(graph::removeEdge);

            kShortestPath = KShortestPath.getKShortestPath(
                    graph,
                    nodeMap.get(business.getSourceNodeId()),
                    nodeMap.get(business.getSinkNodeId()),
                    Collections.emptyList(),
                    50);
        } else {
            kShortestPath = KShortestPath.getKShortestPath(
                    graph,
                    nodeMap.get(business.getSourceNodeId()),
                    nodeMap.get(business.getSinkNodeId()),
                    args.businessSimulationStrategy.getDesignatedPoints().stream()
                            .map(nodeMap::get)
                            .collect(Collectors.toList()),
                    50);
        }

        List<BsBusinessRouteDTO> routes = new ArrayList<>(kShortestPath.size());
        for (GraphPath<BsNodeDO, GraphEdge> path : kShortestPath) {
            BsNodeDO startVertex = path.getStartVertex();
            BsNodeDO endVertex = path.getEndVertex();
            List<BsNodeDO> nodeList = path.getVertexList();
            List<GraphEdge> edgeList = path.getEdgeList();

            BsBusinessRouteDTO route = new BsBusinessRouteDTO();
            route.setBeginNode(new RouteNode(startVertex.getId(), NodeTypeEnum.codeOf(startVertex.getType())));
            route.setEndNode(new RouteNode(endVertex.getId(), NodeTypeEnum.codeOf(endVertex.getType())));
            route.setNodeList(nodeList.stream()
                    .map(node -> new RouteNode(node.getId(), NodeTypeEnum.codeOf(node.getType())))
                    .collect(Collectors.toList()));
            route.setOmsList(new ArrayList<>(edgeList.size()));
            route.setDifferentNodeCrossDomainRelationshipList(new ArrayList<>(edgeList.size()));

            for (GraphEdge graphEdge : edgeList) {
                if (graphEdge.isCrossDomain) {
                    route.getDifferentNodeCrossDomainRelationshipList().add(graphEdge.crossDomainRelationship);
                } else {
                    BsBusinessRouteDTO.RouteOms routeOms = new BsBusinessRouteDTO.RouteOms();
                    routeOms.setId(graphEdge.oms.getId());
                    routeOms.setName(graphEdge.oms.getName());
                    routeOms.setSourceNodeId(graphEdge.oms.getSourceNodeId());
                    routeOms.setSinkNodeId(graphEdge.oms.getSinkNodeId());
                    routeOms.setProtectionMechanism(OmsProtectionMechanismEnum.codeOf(graphEdge.oms.getProtectionMechanism()));
                    Map<LinkRoleEnum, BsLinkDTO> linkMap = new HashMap<>();
                    linkMap.put(LinkRoleEnum.ACTIVE, args.cache.omsIdActiveLinkMap.get(routeOms.getId()));
                    if (args.cache.omsIdStandbyLinkMap.containsKey(routeOms.getId())) {
                        linkMap.put(LinkRoleEnum.STANDBY, args.cache.omsIdStandbyLinkMap.get(routeOms.getId()));
                    }
                    routeOms.setLinkMap(linkMap);
                    route.getOmsList().add(routeOms);
                }
            }

            routes.add(route);
        }
        return routes;
    }

    /**
     * 构建图
     *
     * @param args 仿真参数
     * @return 图
     */
    private Graph<BsNodeDO, GraphEdge> buildGraph(BusinessSimulationArgs args) {
        List<BsNodeDO> nodeCopy = new ArrayList<>(args.cache.nodeMap.values());
        List<BsOmsDO> omsCopy = new ArrayList<>(args.cache.omsMap.values());

        BsBusinessSimulationStrategyDTO businessSimulationStrategy = args.businessSimulationStrategy;

        if (CollectionUtils.isEmpty(businessSimulationStrategy.getDesignatedPoints())
                && CollectionUtils.isEmpty(businessSimulationStrategy.getBypassPoints())) {
            // 没有配置必经点/避开点时，考虑是否需要路由分离
            List<BsBusinessDO> sameGroupBusinessList = this.getBusinessByGroup(businessSimulationStrategy.getBusinessGroupId());

            if (!sameGroupBusinessList.isEmpty()) {
                BsBusinessGroupDO businessGroup = bsBusinessGroupMapper.selectById(businessSimulationStrategy.getBusinessGroupId());

                if (businessGroup.getRouteSeparationPolicy() != null) {
                    RouteSeparationPolicyEnum separationPolicy = RouteSeparationPolicyEnum.codeOf(businessGroup.getRouteSeparationPolicy());

                    for (BsBusinessDO sameGroupBusiness : sameGroupBusinessList) {
                        BsBusinessRouteDTO route = bsBusinessService.getBusinessSimulationRoute(sameGroupBusiness.getId());

                        if (route == null) {
                            continue;
                        }

                        Set<Long> nodeIdSet = route.getNodeList().stream()
                                .map(BsBusinessRouteDTO.RouteNode::getId)
                                .collect(Collectors.toSet());
                        Set<Long> omsIdSet = route.getOmsList().stream()
                                .map(BsBusinessRouteDTO.RouteOms::getId)
                                .collect(Collectors.toSet());

                        if (RouteSeparationPolicyEnum.NODE.equals(separationPolicy)) {
                            nodeCopy.removeIf(node -> nodeIdSet.contains(node.getId()) &&  !args.business.getSourceNodeId().equals(node.getId())&&!args.business.getSinkNodeId().equals(node.getId()));
                        } else if (RouteSeparationPolicyEnum.LINK.equals(separationPolicy)) {
                            omsCopy.removeIf(oms -> omsIdSet.contains(oms.getId()));
                        } else if (RouteSeparationPolicyEnum.SRLG.equals(separationPolicy)) {
                            omsCopy.removeIf(oms -> {
                                for (BsBusinessRouteDTO.RouteOms routeOms : route.getOmsList()) {
                                    if (Objects.equals(oms.getId(), routeOms.getId())) {
                                        return true;
                                    }

                                    BsOmsDO routeOmsDO = args.cache.omsMap.get(routeOms.getId());
                                    Set<Long> srlgOmsIdList = args.cache.srlgMap.getOrDefault(routeOmsDO,
                                            Collections.emptySet());
                                    if (srlgOmsIdList.contains(oms.getId())) {
                                        return true;
                                    }
                                }
                                return false;
                            });
                        }
                    }
                }
            }
        } else if (CollectionUtils.isNotEmpty(businessSimulationStrategy.getBypassPoints())) {
            nodeCopy.removeIf(node -> businessSimulationStrategy.getBypassPoints().contains(node.getId()));
        }

        Map<Long, BsNodeDO> nodeIdMap = nodeCopy.stream()
                .collect(Collectors.toMap(BsNodeDO::getId, Function.identity()));
        omsCopy.removeIf(oms ->
                !nodeIdMap.containsKey(oms.getSourceNodeId()) || !nodeIdMap.containsKey(oms.getSinkNodeId()));

        DefaultUndirectedWeightedGraph<BsNodeDO, GraphEdge> graph = new DefaultUndirectedWeightedGraph<>(GraphEdge.class);
        nodeCopy.forEach(graph::addVertex);
        for (BsOmsDO oms : omsCopy) {
            if (CollectionUtils.isNotEmpty(args.avoidOmsIdList) && args.avoidOmsIdList.contains(oms.getId())) {
                continue;
            }

            BsNodeDO sourceNode = nodeIdMap.get(oms.getSourceNodeId());
            BsNodeDO sinkNode = nodeIdMap.get(oms.getSinkNodeId());

            BsLinkDTO activeLink = args.cache.omsIdActiveLinkMap.get(oms.getId());

            GraphEdge edge = new GraphEdge(oms);
            if (graph.addEdge(sourceNode, sinkNode, edge)) {
                graph.setEdgeWeight(edge, activeLink.getDistance());
            }
        }

        for (Map.Entry<Set<Long>, BsCrossDomainRelationshipDTO> entry : args.cache.crossDomainRelationshipMap.entrySet()) {
            Set<Long> nodeIdSet = entry.getKey();
            BsCrossDomainRelationshipDTO crossDomainRelationship = entry.getValue();

            if (nodeIdSet.size() == 2) {
                BsNodeDO sourceNode = nodeIdMap.get(crossDomainRelationship.getSourceNodeId());
                BsNodeDO sinkNode = nodeIdMap.get(crossDomainRelationship.getSinkNodeId());
                if (sourceNode == null || sinkNode == null) {
                    continue;
                }

                GraphEdge edge = new GraphEdge(crossDomainRelationship);
                if (graph.addEdge(sourceNode, sinkNode, edge)) {
                    graph.setEdgeWeight(edge, CROSS_DOMAIN_RELATIONSHIP_WEIGHT);
                }
            }
        }

        return graph;
    }

    /**
     * 获取由必经点、业务源宿点组成的路径上的OmsId列表<br>
     * 如果必经点、业务源宿点无法组成有效的链路，则返回空集合
     *
     * @param args 业务仿真参数
     * @return oms id list
     */
    private List<Long> getOmsIdListOnPathComposedOfDesignatedPoints(BusinessSimulationArgs args) {
        Set<Long> passingPoint = new HashSet<>(args.businessSimulationStrategy.getDesignatedPoints());
        if (CollectionUtils.isEmpty(passingPoint)) {
            return Collections.emptyList();
        }

        passingPoint.add(args.business.getSourceNodeId());
        passingPoint.add(args.business.getSinkNodeId());

        Map<Set<Long>, BsOmsDO> nodeIdSetOmsMap = new HashMap<>();
        for (BsOmsDO oms : args.cache.omsMap.values()) {
            if (passingPoint.contains(oms.getSourceNodeId()) && passingPoint.contains(oms.getSinkNodeId())) {
                HashSet<Long> nodeIdSet = Sets.newHashSet(oms.getSourceNodeId(), oms.getSinkNodeId());
                BsOmsDO omsDO = nodeIdSetOmsMap.get(nodeIdSet);
                if (omsDO == null
                        || args.cache.omsIdActiveLinkMap.get(oms.getId()).getDistance() > args.cache.omsIdActiveLinkMap.get(omsDO.getId()).getDistance()) {
                    nodeIdSetOmsMap.put(nodeIdSet, oms);
                }
            }
        }

        try {
            List<BsOmsDO> path = PathUtils.sortRelationship(
                    new ArrayList<>(nodeIdSetOmsMap.values()),
                    args.business.getSourceNodeId(), args.business.getSinkNodeId(),
                    BsOmsDO::getSourceNodeId, BsOmsDO::getSinkNodeId);

            return path.stream().map(BsOmsDO::getId).collect(Collectors.toList());
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * 获取相同分组的业务
     *
     * @param groupId 业务分组ID
     * @return 处于该分组的业务集合；
     */
    private List<BsBusinessDO> getBusinessByGroup(Long groupId) {
        if (groupId == null) {
            return Collections.emptyList();
        }

        List<BsBusinessSimulationStrategyDO> simulationStrategyList = businessSimulationStrategyMapper.selectList(
                Wrappers.lambdaQuery(BsBusinessSimulationStrategyDO.class)
                        .eq(BsBusinessSimulationStrategyDO::getBusinessGroupId, groupId));

        List<Long> businessIdList = simulationStrategyList.stream()
                .map(BsBusinessSimulationStrategyDO::getBsBusinessId)
                .collect(Collectors.toList());

        if (businessIdList.isEmpty()) {
            return Collections.emptyList();
        }

        return bsBusinessMapper.selectBatchIds(businessIdList);
    }

    /**
     * 持久化仿真结果
     *
     * @param args               仿真参数
     * @param result             仿真结果
     * @param candidateRouteList 全部候选路由集合
     */
    private void persistentSimulationResult(BusinessSimulationArgs args,
                                            BusinessSimulationResult result,
                                            List<BsBusinessRouteDTO> candidateRouteList) {

        BusinessSimulationStatusEnum simulationStatus;
        if (result != null) {
            simulationStatus = BusinessSimulationStatusEnum.FINISH;
            // 保存波道占用
            this.persistentChannelOccupied(result);

            // 保存中继性能概览
            bsBusinessSimulationResultMapper.insert(this.buildBsBusinessSimulationResult(args, result));

            // 保存候选路由
            this.persistentCandidateRoute(args, candidateRouteList);
        } else {
            simulationStatus = BusinessSimulationStatusEnum.FAIL;
        }

        bsBusinessMapper.update(null, Wrappers.lambdaUpdate(BsBusinessDO.class)
                .eq(BsBusinessDO::getId, args.business.getId())
                .set(BsBusinessDO::getSimulationStatus, simulationStatus.getCode()));
    }

    /**
     * 持久化候选路由占用
     *
     * @param args               仿真参数
     * @param candidateRouteList 候选路由列表
     */
    private void persistentCandidateRoute(BusinessSimulationArgs args, List<BsBusinessRouteDTO> candidateRouteList) {
        List<BsBusinessRouteDO> routeDOList = new ArrayList<>();
        for (int i = 0, candidateRouteListSize = candidateRouteList.size(); i < candidateRouteListSize; i++) {
            BsBusinessRouteDTO route = candidateRouteList.get(i);

            List<ObjectNode> edgeList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(route.getOmsList())) {
                for (BsBusinessRouteDTO.RouteOms oms : route.getOmsList()) {
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.put("isCrossDomain", false);
                    objectNode.put("sourceNodeId", oms.getSourceNodeId());
                    objectNode.put("sinkNodeId", oms.getSinkNodeId());
                    objectNode.put("omsId", oms.getId());

                    edgeList.add(objectNode);
                }
            }
            if (CollectionUtils.isNotEmpty(route.getDifferentNodeCrossDomainRelationshipList())) {
                for (BsCrossDomainRelationshipDTO relationship : route.getDifferentNodeCrossDomainRelationshipList()) {
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.put("isCrossDomain", true);
                    objectNode.put("sourceNodeId", relationship.getSourceNodeId());
                    objectNode.put("sinkNodeId", relationship.getSinkNodeId());

                    edgeList.add(objectNode);
                }
            }

            List<ObjectNode> orderedEdgeList = PathUtils.sortRelationship(
                    edgeList,
                    route.getBeginNode().getId(),
                    route.getEndNode().getId(),
                    node -> node.get("sourceNodeId").longValue(),
                    node -> node.get("sinkNodeId").longValue());

            Long nodeIdFlag = args.business.getSourceNodeId();
            for (int j = 0; j < orderedEdgeList.size(); j++) {
                ObjectNode edge = orderedEdgeList.get(j);
                Long edgeSourceNodeId = edge.get("sourceNodeId").longValue();
                Long edgeSinkNodeId = edge.get("sinkNodeId").longValue();

                BsBusinessRouteDO routeDO = new BsBusinessRouteDO();
                routeDO.setBsProjectId(args.project.getId());
                routeDO.setBsBusinessId(args.business.getId());
                routeDO.setIndex(i);
                routeDO.setSequence(j);

                if (edge.get("isCrossDomain").booleanValue()) {
                    routeDO.setCrossDomain(YesAndNoEnum.YES.getCode());
                } else {
                    routeDO.setCrossDomain(YesAndNoEnum.NO.getCode());
                    routeDO.setBsOmsId(edge.get("omsId").longValue());
                }

                routeDO.setSourceNodeId(nodeIdFlag);
                if (Objects.equals(nodeIdFlag, edgeSourceNodeId)) {
                    routeDO.setSinkNodeId(edgeSinkNodeId);
                } else {
                    routeDO.setSinkNodeId(edgeSourceNodeId);
                }

                routeDOList.add(routeDO);
                nodeIdFlag = routeDO.getSinkNodeId();
            }
        }

        if (!routeDOList.isEmpty()) {
            bsBusinessRouteMapper.insertBatchSomeColumn(routeDOList);
        }
    }

    /**
     * 持久化波道占用
     *
     * @param result 仿真结果
     */
    private void persistentChannelOccupied(BusinessSimulationResult result) {
        // 保存oms波道占用
        if (CollectionUtils.isNotEmpty(result.channel.omsChannelOccupied)) {
            bsOmsChannelMapper.insertBatchSomeColumn(result.channel.omsChannelOccupied);
        }

        // 保存本地维度波道占用
        if (CollectionUtils.isNotEmpty(result.channel.localDimensionChannelOccupied)) {
            bsLocalDimensionChannelMapper.insertBatchSomeColumn(result.channel.localDimensionChannelOccupied);
        }
    }

    private BsBusinessSimulationResultDO buildBsBusinessSimulationResult(BusinessSimulationArgs args,
                                                                         BusinessSimulationResult result) {
        List<Long> omsIdList = result.route.getOmsList().stream()
                .map(BsBusinessRouteDTO.RouteOms::getId)
                .collect(Collectors.toList());

        BsBusinessSimulationResultDO simulationResult = new BsBusinessSimulationResultDO();
        simulationResult.setBsProjectId(args.project.getId());
        simulationResult.setBsBusinessId(args.business.getId());
        simulationResult.setRelayStationCount(
                Optional.ofNullable(result.relay.getRelayNodeIdList()).orElse(Collections.emptyList()).size());
        simulationResult.setBmsCount(simulationResult.getRelayStationCount() + 1);
        simulationResult.setOmsCount(result.route.getOmsList().size());

        simulationResult.setRouteActiveDistance(0L);
        simulationResult.setRouteActiveOtsCount(0);

        List<BsLinkDO> linkList = bsLinkMapper.selectList(Wrappers.lambdaQuery(BsLinkDO.class)
                .in(BsLinkDO::getBsOmsId, omsIdList));

        for (BsLinkDO link : linkList) {
            if (LinkRoleEnum.ACTIVE.getCode() == link.getRole()) {
                simulationResult.setRouteActiveDistance(
                        simulationResult.getRouteActiveDistance() + link.getDistance());
                simulationResult.setRouteActiveOtsCount(
                        simulationResult.getRouteActiveOtsCount() + link.getOaStationNumber() + 1);
            }
        }

        // 所有情况（断纤场景）下所有业务复用段的OSNR值集合
        List<BigDecimal> osnrList = result.relay.getItems().stream()
                .map(BsBusinessRelayOverviewDTO.Item::getBmsList)
                .flatMap(List::stream)
                .map(BsBusinessRelayOverviewDTO.BmsOverview::getOsnrValue)
                .collect(Collectors.toList());

        simulationResult.setOsnrAve(BigDecimalMathUtils.average(osnrList.toArray(new BigDecimal[]{})));
        simulationResult.setOsnrVar(BigDecimalMathUtils.variance(osnrList.toArray(new BigDecimal[]{})));
        simulationResult.setOsnrMin(osnrList.stream().min(BigDecimal::compareTo).orElse(BigDecimal.ZERO));

        // TODO 这几个值业务含义不清晰
        simulationResult.setActiveOverlengthCombinationCount(0);
        simulationResult.setStandbyOverlengthCombinationCount(0);
        simulationResult.setActiveOsnrMin(new BigDecimal("0"));
        simulationResult.setStandbyOsnrMin(new BigDecimal("0"));

        return simulationResult;
    }

    /**
     * 获取缓存
     *
     * @param projectId 项目ID
     * @return 缓存
     */
    protected SimulationCache buildCache(Long projectId) {
        SimulationCache cache = new SimulationCache(projectId);

        // allChannelList
        // TODO 每个OMS波长不同
        List<Integer> allChannelList = new ArrayList<>(80);
        for (int i = 0; i < 80; i++) {
            allChannelList.add(i + 1);
        }
        cache.allChannelList = Collections.unmodifiableList(allChannelList);

        // nodeMap
        List<BsNodeDO> nodeList = bsNodeMapper.selectList(Wrappers.lambdaQuery(BsNodeDO.class)
                .eq(BsNodeDO::getBsProjectId, projectId));
        cache.nodeMap = nodeList.stream().collect(Collectors.toMap(BsNodeDO::getId, Function.identity()));

        // omsMap
        List<BsOmsDO> omsList = bsOmsMapper.selectList(Wrappers.lambdaQuery(BsOmsDO.class)
                .eq(BsOmsDO::getBsProjectId, projectId));
        cache.omsMap = omsList.stream().collect(Collectors.toMap(BsOmsDO::getId, Function.identity()));


        // omsIdActiveLinkMap、omsIdStandbyLinkMap
        List<BsLinkDTO> linkList = bsLinkService.getLinkListByConditions(BsLinkConditions.BsLinkConditionsBuilder.builder()
                .projectId(projectId)
                .build());
        for (BsLinkDTO link : linkList) {
            if (LinkRoleEnum.ACTIVE.equals(link.getRole())) {
                cache.omsIdActiveLinkMap.put(link.getBsOmsId(), link);
            } else if (LinkRoleEnum.STANDBY.equals(link.getRole())) {
                cache.omsIdStandbyLinkMap.put(link.getBsOmsId(), link);
            }
        }

        // nodeOmsMap
        cache.nodeOmsMap = new HashMap<>(nodeList.size());
        for (BsOmsDO oms : omsList) {
            Set<BsOmsDO> sourceNodeOmsSet = cache.nodeOmsMap.computeIfAbsent(
                    cache.nodeMap.get(oms.getSourceNodeId()), key -> new HashSet<>());
            sourceNodeOmsSet.add(oms);

            Set<BsOmsDO> sinkNodeOmsSet = cache.nodeOmsMap.computeIfAbsent(
                    cache.nodeMap.get(oms.getSinkNodeId()), key -> new HashSet<>());
            sinkNodeOmsSet.add(oms);
        }

        // crossDomainRelationshipMap
        List<BsCrossDomainRelationshipDTO> crossDomainList = bsSystemService.getCrossDomainList(projectId);
        crossDomainList.removeIf(crossDomain -> crossDomain.getSourceNodeId().equals(crossDomain.getSinkNodeId()));
        cache.crossDomainRelationshipMap = crossDomainList.stream().collect(Collectors.toMap(
                relationship -> Sets.newHashSet(relationship.getSourceNodeId(), relationship.getSinkNodeId()),
                Function.identity()));

        // nodeLocalDimensionMap
        List<BsLocalDimensionDO> localDimensionList = bsLocalDimensionMapper.selectList(
                Wrappers.lambdaQuery(BsLocalDimensionDO.class).eq(BsLocalDimensionDO::getBsProjectId, projectId));
        cache.nodeLocalDimensionMap = localDimensionList.stream()
                .collect(Collectors.groupingBy(ld -> cache.nodeMap.get(ld.getBsNodeId())));

        // availableLocalDimensionChannel
        for (BsLocalDimensionDO localDimension : localDimensionList) {
            cache.availableLocalDimensionChannel.put(localDimension, new ArrayList<>(allChannelList));
        }

        // availableOmsChannel
        for (BsOmsDO oms : omsList) {
            cache.availableOmsChannel.put(oms, new ArrayList<>(allChannelList));
        }

        // srlgMap
        Map<Long, Set<Long>> srlg = this.getSrlg(projectId);
        for (Map.Entry<Long, Set<Long>> entry : srlg.entrySet()) {
            cache.srlgMap.put(cache.omsMap.get(entry.getKey()), entry.getValue());
        }

        return cache;
    }

    /**
     * 获取SRLG配置
     *
     * @param projectId 项目唯一标识
     * @return Key：oms id，Value：oms id set
     */
    private Map<Long, Set<Long>> getSrlg(Long projectId) {
        List<BsSrlgDO> bsSrlgDOList =
                bsSrlgMapper.selectList(Wrappers.lambdaQuery(BsSrlgDO.class).eq(BsSrlgDO::getBsProjectId, projectId));
        Map<Long, Set<Long>> result = new HashMap<>();
        for (BsSrlgDO bsSrlgDO : bsSrlgDOList) {
            Set<Long> srlgOmsSet = result.computeIfAbsent(bsSrlgDO.getBsOmsId(), key -> new HashSet<>());
            srlgOmsSet.add(bsSrlgDO.getSrlgBsOmsId());
        }
        return result;
    }

    /**
     * 仿真日志包装类
     */
    private static class SimulationLogWrapper {
        BusinessSimulationArgs args;
        List<BsBusinessSimulationLogDO> logs = new ArrayList<>();
        Long startTime;
        BsBusinessSimulationLogDO currentLog = null;

        SimulationLogWrapper(BusinessSimulationArgs args) {
            this.args = args;
        }

        void newLog(String operationProcedure) {
            if (currentLog != null) {
                this.finish(YesAndNoEnum.YES, null);
            }
            currentLog = new BsBusinessSimulationLogDO();
            currentLog.setBsProjectId(args.project.getId());
            currentLog.setBsBusinessId(args.business.getId());
            currentLog.setOperationTime(LocalDateTime.now());
            currentLog.setOperationProcedure(operationProcedure);
            startTime = System.currentTimeMillis();
        }

        void finish(YesAndNoEnum success, String message) {
            if (currentLog == null) {
                return;
            }
            currentLog.setSuccess(success.getCode());
            currentLog.setElapsedTime(System.currentTimeMillis() - startTime);
            currentLog.setMessage(Optional.ofNullable(message).orElse(""));
            logs.add(currentLog);
            log.debug("业务仿真计算 - BusinessId: {}, BusinessName: {}, {}: {}",
                    args.business.getId(), args.business.getName(), currentLog.getOperationProcedure(), currentLog.getMessage());
            currentLog = null;
        }
    }

    /**
     * 仿真过程所需的缓存
     */
    protected static class SimulationCache {
        final Long projectId;
        /**
         * 包含所有节点的集合，Key：nodeId
         */
        Map<Long, BsNodeDO> nodeMap = new HashMap<>();
        /**
         * 包含所有OMS的集合，Key：omsId
         */
        Map<Long, BsOmsDO> omsMap = new HashMap<>();
        /**
         * 包含所有主用链路的集合，Key：oms id，Value：主用链路
         */
        Map<Long, BsLinkDTO> omsIdActiveLinkMap = new HashMap<>();
        /**
         * 包含所有备用链路的集合，Key：oms id，Value：备用链路
         */
        Map<Long, BsLinkDTO> omsIdStandbyLinkMap = new HashMap<>();
        /**
         * 包含所有节点与OMS关联关系的集合，Key：node
         */
        Map<BsNodeDO, Set<BsOmsDO>> nodeOmsMap = new HashMap<>();
        /**
         * 包含所有不同节点跨域连接的节点与跨域连接关联关系的集合，Key：两个跨域节点唯一标识的集合
         */
        Map<Set<Long>, BsCrossDomainRelationshipDTO> crossDomainRelationshipMap = new HashMap<>();

        /**
         * 包含所有本地维度的集合，Key：node
         */
        Map<BsNodeDO, List<BsLocalDimensionDO>> nodeLocalDimensionMap = new HashMap<>();
        /**
         * 包含所有本地维度中<b>可用</b>的波道号的集合
         */
        Map<BsLocalDimensionDO, List<Integer>> availableLocalDimensionChannel = new HashMap<>();
        /**
         * 包含所有OMS中<b>可用</b>的波道号的集合
         */
        Map<BsOmsDO, List<Integer>> availableOmsChannel = new HashMap<>();

        /**
         * 根据项目配置，确定的所有波道号的集合，例如：1~80
         */
        List<Integer> allChannelList = new ArrayList<>();

        /**
         * 当前项目的SRLG配置, Value：oms id set
         */
        Map<BsOmsDO, Set<Long>> srlgMap = new HashMap<>();

        SimulationCache(Long projectId) {
            this.projectId = projectId;
        }

        /**
         * 更新占用
         */
        void update(BsOmsChannelMapper omsChannelMapper, BsLocalDimensionChannelMapper ldChannelMapper) {
            // update oms channel
            List<BsOmsChannelDO> omsChannelList = omsChannelMapper.selectList(Wrappers.lambdaQuery(BsOmsChannelDO.class)
                    .eq(BsOmsChannelDO::getBsProjectId, projectId));
            Map<Long, Set<Integer>> groupedOccupiedOmsChannel = omsChannelList.stream()
                    .collect(Collectors.groupingBy(BsOmsChannelDO::getBsOmsId,
                            Collectors.mapping(BsOmsChannelDO::getChannelIndex, Collectors.toSet())));
            for (Map.Entry<BsOmsDO, List<Integer>> entry : availableOmsChannel.entrySet()) {
                List<Integer> availableChannel = new ArrayList<>(allChannelList);

                Set<Integer> occupiedChannelSet = groupedOccupiedOmsChannel.getOrDefault(entry.getKey().getId(), Collections.emptySet());
                availableChannel.removeIf(occupiedChannelSet::contains);
                entry.setValue(new ArrayList<>(availableChannel));
            }

            // update local dimension channel
            List<BsLocalDimensionChannelDO> ldChannelList = ldChannelMapper.selectList(Wrappers.lambdaQuery(BsLocalDimensionChannelDO.class)
                    .eq(BsLocalDimensionChannelDO::getBsProjectId, projectId));
            Map<Long, Set<Integer>> groupedOccupiedLdChannelList = ldChannelList.stream()
                    .collect(Collectors.groupingBy(BsLocalDimensionChannelDO::getBsLocalDimensionId,
                            Collectors.mapping(BsLocalDimensionChannelDO::getChannelIndex, Collectors.toSet())));
            for (Map.Entry<BsLocalDimensionDO, List<Integer>> entry : availableLocalDimensionChannel.entrySet()) {
                List<Integer> availableChannel = new ArrayList<>(allChannelList);

                Set<Integer> occupiedChannelSet = groupedOccupiedLdChannelList.getOrDefault(entry.getKey().getId(), Collections.emptySet());
                availableChannel.removeIf(occupiedChannelSet::contains);
                entry.setValue(new ArrayList<>(availableChannel));
            }
        }
    }

    /**
     * 业务仿真结果
     */
    private static class BusinessSimulationResult {
        BsBusinessRouteDTO route;
        BsBusinessRelayOverviewDTO relay;
        ChannelSimulationResult channel;
    }

    /**
     * 波道仿真结果
     */
    private static class ChannelSimulationResult {
        Integer sameOmsChannelCount = 0;
        List<BsOmsChannelDO> omsChannelOccupied = new ArrayList<>();
        List<BsLocalDimensionChannelDO> localDimensionChannelOccupied = new ArrayList<>();
    }

    /**
     * 仿真参数
     */
    protected static class BusinessSimulationArgs {
        BsProjectDTO project;
        Long projectId;
        BsBusinessDO business;
        BsProjectConfigurationDTO projectConfiguration;
        BsBusinessSimulationStrategyDTO businessSimulationStrategy;
        List<Long> avoidOmsIdList;
        SimulationCache cache;
    }

    public static class GraphEdge {
        boolean isCrossDomain;
        BsOmsDO oms;
        BsCrossDomainRelationshipDTO crossDomainRelationship;

        public GraphEdge() {

        }

        GraphEdge(BsOmsDO oms) {
            Preconditions.checkNotNull(oms);

            this.isCrossDomain = false;
            this.oms = oms;
        }

        GraphEdge(BsCrossDomainRelationshipDTO crossDomainRelationship) {
            Preconditions.checkNotNull(crossDomainRelationship);

            this.isCrossDomain = true;
            this.crossDomainRelationship = crossDomainRelationship;
        }
    }

    private static class StandardCache {
        /**
         * The Distance standard data.
         */
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> distanceStandardData;
        Map<BusinessSimulationResult, BigDecimal> distanceStandardDataMap;

        /**
         * The Ots standard data.
         */
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> otsStandardData;
        Map<BusinessSimulationResult, BigDecimal> otsStandardDataMap;
        /**
         * The Oms standard data.
         */
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> omsStandardData;
        Map<BusinessSimulationResult, BigDecimal> omsStandardDataMap;
        /**
         * The Relay standard data.
         */
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> relayStandardData;
        Map<BusinessSimulationResult, BigDecimal> relayStandardDataMap;

        /**
         * The Oms utilization data.
         */
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> omsUtilizationData;
        Map<BusinessSimulationResult, BigDecimal> omsUtilizationDataMap;
        /**
         * The Performance data.
         */
        List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> performanceData;
        Map<BusinessSimulationResult, BigDecimal> performanceDataMap;

        /**
         * Instantiates a new Standard cache.
         */
        public StandardCache() {
        }

        /**
         * Instantiates a new Standard cache.
         *
         * @param distanceStandardData the distance standard data
         * @param otsStandardData      the ots standard data
         * @param omsStandardData      the oms standard data
         * @param relayStandardData    the relay standard data
         * @param omsUtilizationData   the oms utilization data
         * @param performanceData      the performance data
         */
        public StandardCache(List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> distanceStandardData,
                             List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> otsStandardData,
                             List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> omsStandardData,
                             List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> relayStandardData,
                             List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> omsUtilizationData,
                             List<DataStandardDeviationUtil.Data<BusinessSimulationResult, BigDecimal>> performanceData) {
            this.distanceStandardData = distanceStandardData;
            this.otsStandardData = otsStandardData;
            this.omsStandardData = omsStandardData;
            this.relayStandardData = relayStandardData;
            this.omsUtilizationData = omsUtilizationData;
            this.performanceData = performanceData;
            this.buildMap();
        }

        private void buildMap() {
            if (CollUtil.isNotEmpty(this.distanceStandardData)) {
                distanceStandardDataMap =
                        this.distanceStandardData.stream()
                                .collect(Collectors.toMap(DataStandardDeviationUtil.Data::getKey,
                                        DataStandardDeviationUtil.Data::getValue));
            }
            if (CollUtil.isNotEmpty(this.otsStandardData)) {
                otsStandardDataMap =
                        this.otsStandardData.stream()
                                .collect(Collectors.toMap(DataStandardDeviationUtil.Data::getKey,
                                        DataStandardDeviationUtil.Data::getValue));
            }
            if (CollUtil.isNotEmpty(this.omsStandardData)) {
                omsStandardDataMap =
                        this.omsStandardData.stream()
                                .collect(Collectors.toMap(DataStandardDeviationUtil.Data::getKey,
                                        DataStandardDeviationUtil.Data::getValue));
            }
            if (CollUtil.isNotEmpty(this.relayStandardData)) {
                relayStandardDataMap =
                        this.relayStandardData.stream()
                                .collect(Collectors.toMap(DataStandardDeviationUtil.Data::getKey,
                                        DataStandardDeviationUtil.Data::getValue));
            }
            if (CollUtil.isNotEmpty(this.omsUtilizationData)) {
                omsUtilizationDataMap =
                        this.omsUtilizationData.stream()
                                .collect(Collectors.toMap(DataStandardDeviationUtil.Data::getKey,
                                        DataStandardDeviationUtil.Data::getValue));
            }
            if (CollUtil.isNotEmpty(this.performanceData)) {
                performanceDataMap =
                        this.performanceData.stream()
                                .collect(Collectors.toMap(DataStandardDeviationUtil.Data::getKey,
                                        DataStandardDeviationUtil.Data::getValue));
            }
        }
    }

    private static class BmsChannelSelectionIterator implements Iterator<List<Integer>> {
        private final YesAndNoEnum allowSwitchingChannel;
        private final List<List<Integer>> availableChannelListForBms;

        /**
         * 用于波长不可变的场景
         */
        private HashMultiset<Integer> multiset;

        /**
         * 用于波长可变的场景
         */
        private Integer[] indexList;

        public BmsChannelSelectionIterator(YesAndNoEnum allowSwitchingChannel, List<List<Integer>> availableChannelListForBms) {
            this.allowSwitchingChannel = allowSwitchingChannel;
            this.availableChannelListForBms = availableChannelListForBms;

            if (YesAndNoEnum.NO.equals(allowSwitchingChannel)) {
                multiset = HashMultiset.create();
                availableChannelListForBms.forEach(multiset::addAll);
            } else {
                indexList = new Integer[availableChannelListForBms.size()];
                Arrays.fill(indexList, 0);
            }
        }

        @Override
        public boolean hasNext() {
            if (YesAndNoEnum.NO.equals(allowSwitchingChannel)) {
                if (!multiset.isEmpty()) {
                    for (Multiset.Entry<Integer> entry : multiset.entrySet()) {
                        if (entry.getCount() == availableChannelListForBms.size()) {
                            return true;
                        }
                    }
                }
                return false;
            } else {
                for (int i = 0; i < indexList.length; i++) {
                    Integer index = indexList[i];
                    List<Integer> channelList = availableChannelListForBms.get(i);
                    if (index >= channelList.size()) {
                        return false;
                    }
                }
                return true;
            }
        }

        @Override
        public List<Integer> next() {
            if (YesAndNoEnum.NO.equals(allowSwitchingChannel)) {
                Set<Multiset.Entry<Integer>> entries = multiset.entrySet();
                Iterator<Multiset.Entry<Integer>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Multiset.Entry<Integer> entry = iterator.next();
                    Integer channel = entry.getElement();
                    int count = entry.getCount();
                    iterator.remove();

                    if (count == availableChannelListForBms.size()) {
                        Integer[] channels = new Integer[availableChannelListForBms.size()];
                        Arrays.fill(channels, channel);
                        return Arrays.asList(channels);
                    }
                }
                throw new NoSuchElementException();
            } else {
                List<Integer> result = new ArrayList<>();
                for (int i = 0; i < indexList.length; i++) {
                    Integer index = indexList[i];
                    List<Integer> channelList = availableChannelListForBms.get(i);
                    if (index < channelList.size()) {
                        result.add(channelList.get(index));
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                for (int i = 0; i < indexList.length; i++) {
                    Integer index = indexList[i];
                    List<Integer> channelList = availableChannelListForBms.get(i);
                    if (index < channelList.size() - 1 || i == indexList.length - 1) {
                        indexList[i] = index + 1;
                        break;
                    } else {
                        indexList[i] = 0;
                    }
                }

                return result;
            }
        }
    }
}
